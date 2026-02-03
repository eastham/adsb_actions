#!/usr/bin/env python3
"""
Batch LOS (Loss of Separation) analysis pipeline for multiple airports.

Processes the busiest GA non-towered airports across a date range,
generating LOS hotspot analysis for each.

Key optimization: Downloads global ADS-B data once per date, reuses across all airports.

Usage:
    python src/tools/batch_los_pipeline.py \
        --start-date 01/15/26 --end-date 01/20/26 \
        --airports examples/busiest_nontowered.txt \
        --max-airports 100 --day-filter weekday

    # Dry run to verify commands without executing:
    python src/tools/batch_los_pipeline.py \
        --start-date 01/15/26 --end-date 01/15/26 \
        --airports examples/busiest_nontowered.txt \
        --max-airports 3 --dry-run
"""

import argparse
import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

# Approximate size of daily ADS-B data from adsb.lol (two tar parts combined)
# Based on recent downloads, each day is approximately 3000 GiB (compressed traces)
ESTIMATED_DAILY_DATA_GB = 3.0
DATA_DIR = Path("data")


def validate_date(date_text):
    """Validate and parse date in mm/dd/yy format."""
    try:
        return datetime.strptime(date_text, '%m/%d/%y')
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Incorrect date format '{date_text}', should be mm/dd/yy")


def load_airport_list(filepath: str, max_airports: int = None) -> list[str]:
    """Load airport codes from text file.

    Handles format like:
        1→DCU
        2→EUL
    or simple one-code-per-line.

    Returns list of FAA codes (3-4 chars).
    """
    airports = []
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Handle "1→DCU" format (arrow separator)
            if '→' in line:
                parts = line.split('→')
                if len(parts) >= 2:
                    code = parts[1].strip()
                    if code:
                        airports.append(code)
            else:
                # Simple one-per-line format
                airports.append(line)

    if max_airports:
        airports = airports[:max_airports]

    return airports


def faa_to_icao(faa_code: str) -> str:
    """Convert FAA 3-letter code to ICAO 4-letter code.

    For US airports, simply prepend 'K'.
    """
    code = faa_code.upper().strip()
    if len(code) >= 4 and code.startswith('K'):
        return code  # Already ICAO
    return f"K{code}"


def is_weekend(date: datetime) -> bool:
    """Return True if date is Saturday (5) or Sunday (6)."""
    return date.weekday() >= 5


def generate_date_range(start: datetime, end: datetime,
                        day_filter: str = 'all') -> list[datetime]:
    """Generate list of dates in range, optionally filtered.

    Args:
        start: Start date (inclusive)
        end: End date (inclusive)
        day_filter: 'all', 'weekday', or 'weekend'

    Returns:
        List of datetime objects
    """
    dates = []
    current = start
    while current <= end:
        include = True
        if day_filter == 'weekday' and is_weekend(current):
            include = False
        elif day_filter == 'weekend' and not is_weekend(current):
            include = False

        if include:
            dates.append(current)
        current += timedelta(days=1)

    return dates


def build_pipeline_command(date: datetime, airport_icao: str,
                           no_cleanup: bool = True) -> str:
    """Build the los_offline_pipeline.py command string."""
    date_str = date.strftime('%m/%d/%y')
    cmd = f"python src/tools/los_offline_pipeline.py {date_str} {airport_icao}"
    if no_cleanup:
        cmd += " --no-cleanup"
    return cmd


def run_command(command: str, dry_run: bool = False) -> int:
    """Execute a shell command, or print if dry_run."""
    if dry_run:
        print(f"[DRY-RUN] {command}")
        return 0

    print(f"🚀 Executing: {command}")
    result = subprocess.run(command, shell=True, text=True)
    if result.returncode != 0:
        print(f"❌ Command failed with return code {result.returncode}")
    return result.returncode


def cleanup_traces():
    """Remove temporary trace directories."""
    for folder in ['traces', 'acas', 'heatmap']:
        path = Path(folder)
        if path.exists():
            print(f"🧹 Cleaning up {folder}/...")
            shutil.rmtree(path)


def check_cached_dates(dates: list[datetime]) -> tuple[list[datetime], list[datetime]]:
    """Check which dates have cached data files.

    Returns:
        Tuple of (cached_dates, uncached_dates)
    """
    cached = []
    uncached = []

    for date in dates:
        date_iso = date.strftime('%Y.%m.%d')
        file_prefix = f"v{date_iso}-planes-readsb-prod-0"
        # Check for both tar parts
        tar_aa = DATA_DIR / f"{file_prefix}.tar.aa"
        tar_ab = DATA_DIR / f"{file_prefix}.tar.ab"

        if tar_aa.exists() and tar_ab.exists():
            cached.append(date)
        else:
            uncached.append(date)

    return cached, uncached


def estimate_download_size(dates: list[datetime]) -> tuple[float, list[datetime], list[datetime]]:
    """Estimate total download size, accounting for cached data.

    Returns:
        Tuple of (estimated_gb, cached_dates, uncached_dates)
    """
    cached, uncached = check_cached_dates(dates)
    estimated_gb = len(uncached) * ESTIMATED_DAILY_DATA_GB
    return estimated_gb, cached, uncached


def main():
    parser = argparse.ArgumentParser(
        description="Batch LOS analysis for multiple airports across date range",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze top 10 airports for one week of weekdays:
  python src/tools/batch_los_pipeline.py \\
      --start-date 01/15/26 --end-date 01/19/26 \\
      --airports examples/busiest_nontowered.txt \\
      --max-airports 10 --day-filter weekday

  # Dry run to verify command generation:
  python src/tools/batch_los_pipeline.py \\
      --start-date 01/15/26 --end-date 01/17/26 \\
      --airports examples/busiest_nontowered.txt \\
      --max-airports 3 --dry-run
"""
    )

    parser.add_argument("--start-date", type=validate_date, required=True,
                        help="Start date in mm/dd/yy format")
    parser.add_argument("--end-date", type=validate_date, required=True,
                        help="End date in mm/dd/yy format")
    parser.add_argument("--airports", type=str, required=True,
                        help="Path to file with airport codes (one per line)")
    parser.add_argument("--max-airports", type=int, default=None,
                        help="Limit to first N airports from the list")
    parser.add_argument("--day-filter", type=str, default="all",
                        choices=["all", "weekday", "weekend"],
                        help="Filter dates by day type (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print commands without executing")

    args = parser.parse_args()

    # Validate date range
    if args.end_date < args.start_date:
        parser.error("End date must be >= start date")

    # Load airports and convert to ICAO
    faa_codes = load_airport_list(args.airports, args.max_airports)
    icao_codes = [faa_to_icao(code) for code in faa_codes]

    # Generate date range
    dates = generate_date_range(args.start_date, args.end_date, args.day_filter)

    if not dates:
        print(f"No dates match filter '{args.day_filter}' in range "
              f"{args.start_date.strftime('%m/%d/%y')} to {args.end_date.strftime('%m/%d/%y')}")
        return

    # Estimate download size
    estimated_gb, cached_dates, uncached_dates = estimate_download_size(dates)

    # Summary
    print(f"=" * 60)
    print(f"Batch LOS Pipeline")
    print(f"=" * 60)
    print(f"Dates: {len(dates)} ({args.day_filter})")
    for d in dates:
        cached_marker = " [cached]" if d in cached_dates else ""
        print(f"  - {d.strftime('%m/%d/%y')} ({d.strftime('%A')}){cached_marker}")
    print(f"Airports: {len(icao_codes)}")
    if len(icao_codes) <= 10:
        print(f"  {', '.join(icao_codes)}")
    else:
        print(f"  {', '.join(icao_codes[:5])} ... {', '.join(icao_codes[-3:])}")
    print(f"Total runs: {len(dates) * len(icao_codes)}")
    print()
    print(f"Download estimate:")
    print(f"  Cached dates: {len(cached_dates)}")
    print(f"  To download:  {len(uncached_dates)} days x ~{ESTIMATED_DAILY_DATA_GB:.0f}GB = ~{estimated_gb:.0f}GB")
    if args.dry_run:
        print()
        print(f"Mode: DRY RUN (no commands will be executed)")
    print(f"=" * 60)
    print()

    # Main processing loop
    total_runs = 0
    failed_runs = []

    for date in dates:
        print(f"\n{'=' * 60}")
        print(f"Processing date: {date.strftime('%m/%d/%y')} ({date.strftime('%A')})")
        print(f"{'=' * 60}")

        for i, icao in enumerate(icao_codes):
            # Use --no-cleanup for all but the last airport of each date
            is_last_airport = (i == len(icao_codes) - 1)
            no_cleanup = not is_last_airport

            cmd = build_pipeline_command(date, icao, no_cleanup=no_cleanup)
            returncode = run_command(cmd, dry_run=args.dry_run)
            total_runs += 1

            if returncode != 0:
                failed_runs.append((date.strftime('%m/%d/%y'), icao))

        # Cleanup after last airport (if not dry-run)
        if not args.dry_run:
            cleanup_traces()

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Batch Complete")
    print(f"{'=' * 60}")
    print(f"Total runs: {total_runs}")
    print(f"Failed: {len(failed_runs)}")
    if failed_runs:
        print("Failed runs:")
        for date_str, icao in failed_runs:
            print(f"  - {date_str} {icao}")


if __name__ == "__main__":
    main()
