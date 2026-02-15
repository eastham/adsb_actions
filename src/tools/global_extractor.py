#!/usr/bin/env python3

# extract data subset for faster development iteration. Usage:
# python3 src/tools/global_extractor.py --start-date 06/01/25 --end-date 06/30/25 --day-filter weekday
# starting from JSONL global.gz files, extract a smaller subset of points that 
# are near a given point, using the latlongring rule in the YAML file. 

import os
from pathlib import Path
from batch_los_pipeline import (download_tar_parts, convert_traces_global,
                                extract_traces, shard_global_to_airports,
                                load_airport_info)
from batch_helpers import (generate_date_range, validate_date)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download ADS-B data from ADSB.LOL for specified dates")
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD inclusive)')
    parser.add_argument('--day-filter', choices=['all', 'weekday', 'weekend'], default='all', help='Filter dates by day type (default: all)')
    parser.add_argument('--data-dir', default='data/adsb_lol', help='Directory to save downloaded data')
    parser.add_argument('--dry-run', action='store_true', help='Print dates to download without actually downloading')
    parser.add_argument('--airports', nargs='*', help='ICAO codes to shard after extraction (e.g. KWVI KMOD)')
    args = parser.parse_args()

    start_date = validate_date(args.start_date)
    end_date = validate_date(args.end_date)
    dates = generate_date_range(start_date, end_date, args.day_filter)

    # Load airport info for sharding if requested
    airport_info = {}
    if args.airports:
        for icao in args.airports:
            airport_info[icao] = load_airport_info(icao)

    # python3  src/analyzers/simple_monitor.py --sorted-file data/global_060825.gz  examples/KMOD/strip_global.yaml
    #         f"python src/tools/convert_traces.py {traces_dir} -o {local_temp} --progress 100")

    for date in dates:
        print (f"*** Processing date: {date}")
        date_str = date.strftime('%m%d%y')
        date_iso = date.strftime('%Y.%m.%d')

        input_file = f"data/global_{date_str}.gz"

        if not os.path.exists(input_file):
            # extract global data from tar file
            if args.dry_run:
                print(f"Dry run: Would extract global data for {date_str}")
            else:
                # make global sorted JSONL
                result = extract_traces(date)
                if not result:
                    print(f"Failed to extract global data for {date_str}, skipping")
                    continue
                convert_traces_global(date)
    
        # extract a subset of the global file for dev iteration.
        if False:
            output_file = "output/KMOD_100nm.gz"
            destination_file = f"data/KMOD_100nm_{date_str}.gz"

            # if output file already exists, skip
            if os.path.exists(destination_file):
                print(f"Final destination file {destination_file} already exists, skipping")
                continue
            else:
                print(f"Output file {destination_file} does not exist, processing...")
            print(f"Extracting local data for {date_str} to {output_file}...")
            command_process = f"python3  src/analyzers/simple_monitor.py --sorted-file {input_file}  examples/KMOD/strip_global.yaml"
            command_mv = f"mv {output_file} {destination_file}"
            if args.dry_run:
                print(f"Would run: {command_process}")
                print(f"Would run: {command_mv}")
            else:
                os.system(command_process)
                os.system(command_mv)

        # Shard global data into per-airport files if airports specified
        if args.airports:
            global_gz = Path(input_file)
            result = shard_global_to_airports(
                global_gz, args.airports, date_str, airport_info,
                dry_run=args.dry_run)
            if result != 0:
                print(f"❌ Shard pass failed for {date_str}")

