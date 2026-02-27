#!/usr/bin/env python3

# extract data subset for faster development iteration. Usage:
# python3 src/tools/global_extractor.py --start-date 06/01/25 --end-date 06/30/25 --airports examples/manual_request_airports.txt

# starting from JSONL global.gz files, extract a smaller subset of points that
# are near a given point, using the latlongring rule in the YAML file.

import multiprocessing
import os
import signal
import shutil
import subprocess
import sys
import time
from pathlib import Path

from batch_los_pipeline import (convert_traces_global,
                                download_tar_parts, extract_traces,
                                shard_global_to_airports, load_airport_info)
from batch_helpers import (generate_date_range, validate_date, load_airport_list,
                           faa_to_icao, CONUS_YAML_TEMPLATE)

# Pool ref for cleanup on Ctrl-C (mutable container avoids global declaration issues)
_state = {"pool": None}


def _worker_init():
    """Restore default SIGINT in workers so subprocess children are killable."""
    signal.signal(signal.SIGINT, signal.SIG_DFL)


def _extract_conus_worker(args):
    """Worker: extract CONUS subset, then optionally shard into per-airport files."""
    date_str, input_file, destination_file, airport_codes, airport_info, force_shard = args

    # Extract CONUS subset if not already done
    if not os.path.exists(destination_file):
        output_file = f"output/CONUS_{date_str}.gz"
        os.makedirs("output", exist_ok=True)

        # Write a per-worker YAML with the unique output path
        yaml_content = CONUS_YAML_TEMPLATE.format(output_file=output_file)
        yaml_path = f"output/conus_{date_str}.yaml"
        with open(yaml_path, 'w') as f:
            f.write(yaml_content)

        command = (f"python3 src/analyzers/simple_monitor.py "
                   f"--sorted-file {input_file} {yaml_path}")
        result = subprocess.run(command, shell=True)
        # os.remove(yaml_path)

        print(f"  Finished CONUS extraction for {date_str}, return code {result.returncode}")

        print(f"  Moving file to final destination {destination_file}...")
        shutil.move(output_file, destination_file)

    # Shard into per-airport files if airports specified
    if airport_codes:
        rc = shard_global_to_airports(
            Path(destination_file), airport_codes, date_str, airport_info,
            force=force_shard)
        if rc != 0:
            return date_str, False, "shard failed"

    return date_str, True, None


if __name__ == "__main__":
    import argparse
    t_start = time.monotonic()
    parser = argparse.ArgumentParser(description="Download ADS-B data from ADSB.LOL for specified dates")
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD inclusive)')
    parser.add_argument('--day-filter', choices=['all', 'weekday', 'weekend'], default='all', help='Filter dates by day type (default: all)')
    parser.add_argument('--data-dir', default='data/adsb_lol', help='Directory to save downloaded data')
    parser.add_argument('--dry-run', action='store_true', help='Print dates to download without actually downloading')
    parser.add_argument('--airports', help='ICAO codes or path to airport list file (e.g. KWVI,KMOD or airports.txt)')
    parser.add_argument('--force-shard', action='store_true', help='Overwrite existing per-airport shard files')
    parser.add_argument('--no-download', action='store_true', help='Skip download step, use existing tar files')
    args = parser.parse_args()

    start_date = validate_date(args.start_date)
    end_date = validate_date(args.end_date)
    dates = generate_date_range(start_date, end_date, args.day_filter)

    def _cleanup_handler(_signum, _frame):
        if _state["pool"]:
            _state["pool"].terminate()
            _state["pool"].join()
        print("\nInterrupted.")
        sys.exit(1)

    signal.signal(signal.SIGINT, _cleanup_handler)
    signal.signal(signal.SIGTERM, _cleanup_handler)

    # Load airport info for sharding if requested
    airport_info = {}
    airport_codes = []
    if args.airports:
        if os.path.isfile(args.airports):
            raw_codes = load_airport_list(args.airports)
        else:
            raw_codes = [a.strip() for a in args.airports.split(',')]
        airport_codes = [faa_to_icao(code) for code in raw_codes]
        for icao in airport_codes:
            airport_info[icao] = load_airport_info(icao)

    # Phase 1: Ensure global files exist (sequential — involves downloading)
    for date in dates:
        date_str = date.strftime('%m%d%y')
        input_file = f"data/global_{date_str}.gz"

        if not os.path.exists(input_file):
            if args.dry_run:
                print(f"Dry run: Would download/extract global data for {date_str}")
            else:
                # Download tar parts if needed (must go to data/ where extract_traces looks)
                if not args.no_download:
                    print(f"Downloading tar parts for {date_str}...")
                    if not download_tar_parts(date, data_dir="data"):
                        print(f"Failed to download tar parts for {date_str}, skipping")
                        continue

                print(f"Extracting global data for {date_str}...")
                result = extract_traces(date)
                if not result:
                    print(f"Failed to extract global data for {date_str}, skipping")
                    continue
                convert_traces_global(date)

    # Phase 2: CONUS extraction + optional sharding (parallelized)
    extract_args = []
    for date in dates:
        date_str = date.strftime('%m%d%y')
        input_file = f"data/global_{date_str}.gz"
        destination_file = f"data/CONUS_{date_str}.gz"

        if not os.path.exists(input_file):
            print(f"Global file {input_file} missing, skipping")
            continue
        if args.dry_run:
            print(f"Would extract CONUS data: {input_file} -> {destination_file}")
            continue

        extract_args.append((date_str, input_file, destination_file,
                             airport_codes, airport_info, args.force_shard))

    if extract_args:
        num_workers = min(multiprocessing.cpu_count(), len(extract_args))
        print(f"Extracting CONUS data for {len(extract_args)} dates "
              f"using {num_workers} workers...")

        pool = multiprocessing.Pool(processes=num_workers,
                                    initializer=_worker_init)
        _state["pool"] = pool
        try:
            for date_str, success, err in pool.imap_unordered(
                    _extract_conus_worker, extract_args):
                if success:
                    print(f"  Completed {date_str}")
                else:
                    print(f"  Failed {date_str}: {err}")
        except KeyboardInterrupt:
            print("\nInterrupted, terminating workers...")
            pool.terminate()
        finally:
            pool.terminate()
            pool.join()

    elapsed = time.monotonic() - t_start
    print(f"\nTotal elapsed time: {elapsed/60:.1f} minutes")
