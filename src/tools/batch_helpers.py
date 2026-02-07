"""Helper utilities for batch LOS pipeline processing."""

import json
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path


class SimpleTimer:
    """Simple timing utility to track phase durations."""

    def __init__(self):
        self.timings = {}
        self.start_times = {}

    def start(self, phase_name):
        """Start timing a phase."""
        self.start_times[phase_name] = time.time()

    def end(self, phase_name):
        """End timing a phase and store duration."""
        if phase_name in self.start_times:
            elapsed = time.time() - self.start_times[phase_name]
            if phase_name not in self.timings:
                self.timings[phase_name] = []
            self.timings[phase_name].append(elapsed)
            del self.start_times[phase_name]
            return elapsed
        return None

    def save_report(self, output_path='timing_report.json'):
        """Save timing report to JSON file."""
        # Aggregate timings
        aggregated = {}
        for phase_name, times in self.timings.items():
            aggregated[phase_name] = {
                'total_seconds': sum(times),
                'count': len(times),
                'avg_seconds': sum(times) / len(times) if times else 0
            }

        # Use total_pipeline as the authoritative total if available,
        # otherwise sum all phases
        if 'total_pipeline' in aggregated:
            total_seconds = aggregated['total_pipeline']['total_seconds']
        else:
            total_seconds = sum(t['total_seconds'] for t in aggregated.values())

        report = {
            'total_seconds': total_seconds,
            'total_minutes': total_seconds / 60,
            'phases': aggregated
        }

        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)

        print(f"\n{'=' * 60}")
        print(f"Timing Report saved to {output_path}")
        print(f"{'=' * 60}")
        print(f"Total time: {total_seconds / 60:.1f} minutes ({total_seconds:.1f} seconds)")
        print()

        # Sort by total time descending
        sorted_phases = sorted(aggregated.items(),
                              key=lambda x: x[1]['total_seconds'],
                              reverse=True)

        for phase_name, stats in sorted_phases:
            pct = 100 * stats['total_seconds'] / total_seconds if total_seconds > 0 else 0
            if stats['count'] > 1:
                print(f"  {phase_name}: {stats['total_seconds']:.1f}s "
                      f"({pct:.1f}%) - {stats['count']} runs, "
                      f"avg {stats['avg_seconds']:.1f}s")
            else:
                print(f"  {phase_name}: {stats['total_seconds']:.1f}s ({pct:.1f}%)")
        print(f"{'=' * 60}")


def faa_to_icao(faa_code: str) -> str:
    """Convert FAA 3-letter code to ICAO 4-letter code.

    For US airports, prepend 'K' only for 3-letter all-alpha codes.
    Alphanumeric codes (e.g., S43, 1R8, M37) stay as-is since they
    don't follow the K-prefix convention.
    """
    code = faa_code.upper().strip()
    if len(code) >= 4 and code.startswith('K'):
        return code  # Already ICAO
    # Only add K prefix to 3-letter all-alphabetic codes
    if len(code) == 3 and code.isalpha():
        return f"K{code}"
    return code


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


def compute_bounds(center_lat: float, center_lon: float,
                   radius_nm: float) -> tuple:
    """Compute SW and NE corners from center point and radius in nautical miles.

    Returns:
        Tuple of (sw_lat, sw_lon, ne_lat, ne_lon)
    """
    # Import here to avoid circular dependency
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from adsb_actions.geo_helpers import nm_to_lat_lon_offsets

    lat_offset, lon_offset = nm_to_lat_lon_offsets(radius_nm, center_lat)
    sw_lat = center_lat - lat_offset
    sw_lon = center_lon - lon_offset
    ne_lat = center_lat + lat_offset
    ne_lon = center_lon + lon_offset
    return sw_lat, sw_lon, ne_lat, ne_lon


def run_command(command: str, dry_run: bool = False, quiet_fail: bool = False) -> int:
    """Execute a shell command, or print if dry_run.

    Args:
        command: Shell command to run
        dry_run: If True, just print the command
        quiet_fail: If True, don't print error message on non-zero return
    """
    if dry_run:
        print(f"[DRY-RUN] {command}")
        return 0

    print(f"🚀 Executing: {command}")
    result = subprocess.run(command, shell=True, text=True)
    if result.returncode != 0 and not quiet_fail:
        print(f"❌ Command failed with return code {result.returncode}")
    return result.returncode


def estimate_download_size(dates: list[datetime], data_dir: Path,
                           daily_data_gb: float) -> tuple[float, list[datetime], list[datetime]]:
    """Estimate total download size, accounting for cached data.

    Args:
        dates: List of dates to check
        data_dir: Directory where cached data is stored
        daily_data_gb: Estimated size per day in GB

    Returns:
        Tuple of (estimated_gb, cached_dates, uncached_dates)
    """
    cached = []
    uncached = []

    for date in dates:
        date_iso = date.strftime('%Y.%m.%d')
        file_prefix = f"v{date_iso}-planes-readsb-prod-0"
        tar_aa = data_dir / f"{file_prefix}.tar.aa"
        tar_ab = data_dir / f"{file_prefix}.tar.ab"

        if tar_aa.exists() and tar_ab.exists():
            cached.append(date)
        else:
            uncached.append(date)

    estimated_gb = len(uncached) * daily_data_gb
    return estimated_gb, cached, uncached


def print_pipeline_summary(dates: list[datetime], cached_dates: list[datetime],
                          uncached_dates: list[datetime], icao_codes: list[str],
                          estimated_gb: float, daily_data_gb: float,
                          day_filter: str, skip_download: bool = False,
                          dry_run: bool = False, aggregate_only: bool = False):
    """Print batch pipeline execution summary.

    Args:
        dates: All dates to process
        cached_dates: Dates with cached data
        uncached_dates: Dates needing download
        icao_codes: List of airport ICAO codes
        estimated_gb: Estimated download size in GB
        daily_data_gb: Size per day in GB
        day_filter: Day filter type ('all', 'weekday', 'weekend')
        skip_download: Whether download is being skipped
        dry_run: Whether this is a dry run
        aggregate_only: Whether only aggregation will run
    """
    print(f"{'=' * 60}")
    print(f"Batch LOS Pipeline (two-pass)")
    print(f"{'=' * 60}")
    print(f"Dates: {len(dates)} ({day_filter})")
    for d in dates:
        cached_marker = " [cached]" if d in cached_dates else ""
        print(f"  - {d.strftime('%m/%d/%y')} ({d.strftime('%A')}){cached_marker}")
    print(f"Airports: {len(icao_codes)}")
    if len(icao_codes) <= 10:
        print(f"  {', '.join(icao_codes)}")
    else:
        print(f"  {', '.join(icao_codes[:5])} ... {', '.join(icao_codes[-3:])}")
    print(f"Total analysis runs: {len(dates) * len(icao_codes)}")
    print()
    if not skip_download:
        print(f"Download estimate:")
        print(f"  Cached dates: {len(cached_dates)}")
        print(f"  To download:  {len(uncached_dates)} days x "
              f"~{daily_data_gb:.0f}GB = ~{estimated_gb:.0f}GB")
    else:
        print(f"Mode: SKIP DOWNLOAD (using existing sharded files)")
    if dry_run:
        print(f"Mode: DRY RUN (no commands will be executed)")
    if aggregate_only:
        print(f"Mode: AGGREGATE ONLY (skipping all processing)")
    print(f"{'=' * 60}")
    print()


def print_completion_summary(dates: list[datetime], icao_codes: list[str],
                            failed_runs: list):
    """Print final completion summary with failures.

    Args:
        dates: All dates processed
        icao_codes: All airports processed
        failed_runs: List of (date_str, icao, stage) tuples for failures
    """
    print(f"\n{'=' * 60}")
    print(f"Batch Complete")
    print(f"{'=' * 60}")
    print(f"Total airport-date runs: {len(dates) * len(icao_codes)}")
    print(f"Failed: {len(failed_runs)}")
    if failed_runs:
        print("Failed runs:")
        for entry in failed_runs:
            if len(entry) == 3:
                date_str, icao, stage = entry
                print(f"  - {date_str} {icao} ({stage})")
            else:
                print(f"  - {entry}")
