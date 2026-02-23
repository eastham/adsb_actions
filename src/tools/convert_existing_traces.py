#!/usr/bin/env python3

"""Convert already-untarred trace directories under ~/adsb_data/202[2-5]/
into data/global_MMDDYY.gz files using convert_traces.py.

Expects directory structure: ~/adsb_data/YYYY/MM/DD/traces/

Usage:
    python src/tools/convert_existing_traces.py [--dry-run] [--data-root ~/adsb_data]
"""

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path


DATA_DIR = Path("data")


def find_trace_dirs(data_root: Path) -> list[tuple[str, Path]]:
    """Find all traces/ directories and return (MMDDYY, traces_path) pairs,
    sorted by date."""
    results = []
    for year_dir in sorted(data_root.glob("202[2-9]")):
        year = year_dir.name
        for month_dir in sorted(year_dir.iterdir()):
            if not month_dir.is_dir() or not month_dir.name.isdigit():
                continue
            for day_dir in sorted(month_dir.iterdir()):
                if not day_dir.is_dir() or not day_dir.name.isdigit():
                    continue
                traces_dir = day_dir / "traces"
                if traces_dir.is_dir():
                    mm = month_dir.name.zfill(2)
                    dd = day_dir.name.zfill(2)
                    yy = year[2:]  # 2024 -> 24
                    date_str = f"{mm}{dd}{yy}"
                    results.append((date_str, traces_dir))
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Convert untarred trace dirs to global_MMDDYY.gz files")
    parser.add_argument('--data-root', default=os.path.expanduser('~/adsb_data'),
                        help='Root directory containing 202X year folders')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print what would be done without running')
    args = parser.parse_args()

    data_root = Path(args.data_root)
    if not data_root.is_dir():
        print(f"Error: {data_root} is not a directory")
        sys.exit(1)

    trace_dirs = find_trace_dirs(data_root)
    if not trace_dirs:
        print(f"No traces/ directories found under {data_root}")
        sys.exit(1)

    print(f"Found {len(trace_dirs)} trace directories")
    DATA_DIR.mkdir(exist_ok=True)

    t_start = time.monotonic()
    converted = 0
    skipped = 0
    failed = 0

    for date_str, traces_path in trace_dirs:
        output_file = DATA_DIR / f"brc_{date_str}.gz"

        if output_file.exists():
            print(f"  Skipping {date_str}: {output_file} already exists")
            skipped += 1
            continue

        if args.dry_run:
            print(f"  Would convert {traces_path} -> {output_file}")
            continue

        print(f"  Converting {traces_path} -> {output_file}...")
        # Write to /tmp first then move, matching convert_traces_global pattern
        tmp_file = Path("/tmp") / output_file.name
        result = subprocess.run(
            f"python src/tools/convert_traces.py {traces_path} -o {tmp_file} --progress 100",
            shell=True)

        if result.returncode != 0:
            print(f"  FAILED for {date_str}")
            # Clean up partial output
            tmp_file.unlink(missing_ok=True)
            failed += 1
            continue

        # Move to final location
        shutil.move(tmp_file, output_file)
        converted += 1
        print(f"  Done: {output_file}")

    elapsed = time.monotonic() - t_start
    print(f"\nResults: {converted} converted, {skipped} skipped, {failed} failed")
    print(f"Elapsed: {elapsed/60:.1f} minutes")


if __name__ == "__main__":
    main()
