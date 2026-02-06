#!/usr/bin/env python3
"""
Batch LOS (Loss of Separation) analysis pipeline for multiple airports.

Two-pass architecture processes all airports efficiently per date:

  Pass 1 (shard): Download global ADS-B data, extract, convert to sorted
  JSONL (once), then stream through adsb_actions with a multi-airport YAML.
  Each rule uses latlongring + emit_jsonl to write matching points to
  per-airport gzipped JSONL files. No resampling.

  Pass 2 (analyze): Per-airport prox_analyze_from_files.py --resample
  --animate-los on each sharded file. Standard single-airport LOS analysis.

  Post-processing: Cross-date CSV aggregation per airport -> visualizer
  produces one map per airport showing all LOS events across all dates.

Performance optimization: The convert/merge phase writes to local /tmp for
speed and reliability (avoiding network I/O issues), then backgrounds the
copy to network storage. The local temp file is cleaned up after sharding.

Usage:
    python src/tools/batch_los_pipeline.py \\
        --start-date 01/15/26 --end-date 01/20/26 \\
        --airports examples/busiest_nontowered.txt \\
        --max-airports 100 --day-filter weekday

    # Dry run to verify commands without executing:
    python src/tools/batch_los_pipeline.py \\
        --start-date 01/15/26 --end-date 01/15/26 \\
        --airports examples/busiest_nontowered.txt \\
        --max-airports 3 --dry-run

    # Skip download/extract, just re-run analysis on existing sharded files:
    python src/tools/batch_los_pipeline.py \\
        --start-date 01/15/26 --end-date 01/15/26 \\
        --airports examples/busiest_nontowered.txt \\
        --max-airports 3 --skip-download
"""

import argparse
import os
import re
import shutil
import subprocess
import requests
from datetime import datetime
from pathlib import Path

import generate_airport_config
from batch_helpers import (
    SimpleTimer,
    faa_to_icao,
    generate_date_range,
    compute_bounds,
    run_command,
    estimate_download_size,
    print_pipeline_summary,
    print_completion_summary,
)

# Approximate size of daily ADS-B data from adsb.lol (two tar parts combined)
ESTIMATED_DAILY_DATA_GB = 3.0
DATA_DIR = Path("data")
BASE_DIR = Path("examples/generated")
FT_MAX_ABOVE_AIRPORT = 4000   # analysis ceiling relative to field elevation
FT_MIN_BELOW_AIRPORT = -200   # negative AGL offset excludes ground traffic
ANALYSIS_RADIUS_NM = 5


def validate_date(date_text):
    """Validate and parse date in mm/dd/yy format."""
    try:
        return datetime.strptime(date_text, '%m/%d/%y')
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Incorrect date format '{date_text}', should be mm/dd/yy")


def load_airport_list(filepath: str, max_airports: int = None) -> list[str]:
    """Load airport codes from text file (one code per line).

    Extracts the first 2-4 character alphanumeric token from each line,
    skipping blank lines and comments.

    Returns list of FAA/ICAO codes.
    """
    airports = []
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            match = re.match(r'[A-Za-z0-9]{2,4}', line)
            if match:
                airports.append(match.group(0))

    if max_airports:
        airports = airports[:max_airports]

    return airports


def cleanup_traces():
    """Remove temporary trace directories from examples/generated/."""
    for folder in ['traces', 'acas', 'heatmap']:
        path = BASE_DIR / folder
        if path.exists():
            print(f"🧹 Cleaning up {path}/...")
            shutil.rmtree(path)


def load_airport_info(icao: str) -> tuple[float, float, int]:
    """Load airport lat, lon, field elevation from OurAirports data.

    Returns:
        Tuple of (lat, lon, field_elevation_ft)
    """
    airport = generate_airport_config.load_airport(icao)
    # If K-prefixed lookup fails, try without K (for Alaska PA* airports etc.)
    if not airport and icao.startswith('K') and len(icao) == 4:
        airport = generate_airport_config.load_airport(icao[1:])
    if not airport:
        raise ValueError(f"Airport {icao} not found in database")
    lat = float(airport.get('latitude_deg') or 0)
    lon = float(airport.get('longitude_deg') or 0)
    field_alt = int(float(airport.get('elevation_ft') or 0))
    return lat, lon, field_alt


def generate_multi_airport_yaml(icao_codes: list[str], date_compact: str,
                                airport_info: dict) -> str:
    """Generate a multi-airport shard YAML with latlongring + emit_jsonl rules.

    Each airport gets one rule that spatially filters points within
    ANALYSIS_RADIUS_NM and writes them to a per-airport gzipped JSONL file.
    No altitude filter or proximity — just spatial sharding.

    Args:
        icao_codes: List of ICAO airport codes
        date_compact: Date string in MMDDYY format for output filenames
        airport_info: Dict mapping ICAO -> (lat, lon, field_alt)

    Returns:
        YAML string
    """
    lines = ["# Multi-airport shard YAML (auto-generated)", "rules:"]

    for icao in icao_codes:
        lat, lon, _ = airport_info[icao]
        output_path = f"{BASE_DIR}/{icao}/{date_compact}_{icao}.gz"
        lines.append(f"  {icao}_shard:")
        lines.append(f"    conditions:")
        lines.append(f"      latlongring: [{ANALYSIS_RADIUS_NM}, {lat}, {lon}]")
        lines.append(f"    actions:")
        lines.append(f"      emit_jsonl: {output_path}")

    return "\n".join(lines) + "\n"


def download_tar_parts(date_obj: datetime, force: bool = False) -> bool:
    """Download the two tar archive parts for a date, from the 
    adsb.lol GitHub releases.

    Returns True if both parts are available (downloaded or cached).
    """
    date_iso = date_obj.strftime('%Y.%m.%d')
    full_year = date_obj.strftime('%Y')
    file_prefix = f"v{date_iso}-planes-readsb-prod-0"

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    for ext in ['aa', 'ab']:
        local_file = DATA_DIR / f"{file_prefix}.tar.{ext}"
        if local_file.exists() and not force:
            print(f"✅ {local_file.name} exists. Skipping download.")
            continue

        print(f"📥 Downloading {local_file.name}...")
        url = (f"https://github.com/adsblol/globe_history_{full_year}"
               f"/releases/download/v{date_iso}-planes-readsb-prod-0"
               f"/{file_prefix}.tar.{ext}")
        print(f"Downloading from {url}...")
        r = requests.get(url, stream=True)

        if r.status_code == 404:
            url = url.replace("prod-0", "prod-0tmp")
            print(f"Retrying download from {url}...")
            r = requests.get(url, stream=True)

        if r.status_code == 200:
            with open(local_file, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        else:
            print(f"⚠️ Could not download {local_file.name} "
                  f"(Status: {r.status_code})")
            return False

    return True


def extract_traces(date_obj: datetime) -> bool:
    """Extract trace archives to examples/generated/ directory.

    Returns True on success.
    """
    date_iso = date_obj.strftime('%Y.%m.%d')
    file_prefix = f"v{date_iso}-planes-readsb-prod-0"

    # Clean old trace data before extract
    extract_base = BASE_DIR  # examples/generated
    for folder in ['traces', 'acas', 'heatmap']:
        folder_path = extract_base / folder
        if folder_path.exists():
            shutil.rmtree(folder_path)

    archive_pattern = DATA_DIR / f"{file_prefix}.tar.a*"
    print(f"📦 Extracting tar archives to {extract_base}/...")
    result = run_command(
        f"cat {archive_pattern} | tar --options read_concatenated_archives -xf - -C {extract_base} --exclude='License*.txt'")
    return result == 0


def convert_traces_global(date_obj: datetime) -> Path:
    """Run convert_traces.py without spatial filter to produce global sorted JSONL.

    Writes to local temp directory first for speed/reliability, then backgrounds
    the copy to network storage.

    Returns:
        Path to the global sorted JSONL file (local temp path for immediate use).
    """
    date_compact = date_obj.strftime('%m%d%y')

    # Use local temp directory for fast, reliable I/O
    local_temp = Path("/tmp") / f"global_{date_compact}.gz"
    network_final = DATA_DIR / f"global_{date_compact}.gz"

    print(f"⚙️ Converting traces to {local_temp} (local temp)...")
    traces_dir = BASE_DIR / "traces"
    result = run_command(
        f"python src/tools/convert_traces.py {traces_dir} -o {local_temp} --progress 100")
    if result != 0:
        raise RuntimeError(f"convert_traces.py failed for {date_compact}")

    # Background copy to network storage (don't block pipeline)
    print(f"📦 Copying {local_temp.name} to network storage in background...")
    run_command(
        f"cp '{local_temp}' '{network_final}' && echo '✅ Network copy complete: {network_final.name}' &",
        dry_run=False)

    # Return local path for immediate use by shard pass
    return local_temp


def run_shard_pass(global_gz: Path, shard_yaml_path: Path,
                   dry_run: bool = False) -> int:
    """Pass 1: Stream global JSONL through adsb_actions with shard YAML.

    Each airport's latlongring + emit_jsonl rule writes matching points
    to per-airport files.
    """
    cmd = (f"python3 src/analyzers/prox_analyze_from_files.py "
           f"--yaml {shard_yaml_path} --sorted-file {global_gz}")
    return run_command(cmd, dry_run=dry_run)


def run_airport_analysis(icao: str, date_compact: str,
                         airport_info: dict,
                         dry_run: bool = False) -> int:
    """Pass 2: Per-airport LOS analysis with resampling and animation.

    Returns command return code.
    """
    lat, lon, field_alt = airport_info[icao]
    airport_dir = BASE_DIR / icao
    trace_gz = airport_dir / f"{date_compact}_{icao}.gz"

    if not trace_gz.exists():
        print(f"⚠️ No sharded data for {icao} on {date_compact}, skipping.")
        return 0

    # Check if file is empty (gzip with no content)
    if trace_gz.stat().st_size < 30:
        print(f"⚠️ Sharded file for {icao} on {date_compact} is empty, skipping.")
        return 0

    # Generate per-airport prox YAML if needed
    airport_yaml = airport_dir / "prox_analyze_from_files.yaml"
    if not airport_yaml.exists():
        print(f"⚙️ Generating airport YAML at {airport_yaml}...")
        yaml_text = generate_airport_config.generate_prox_yaml(
            icao, field_alt + FT_MAX_ABOVE_AIRPORT, field_alt - FT_MIN_BELOW_AIRPORT)
        airport_dir.mkdir(parents=True, exist_ok=True)
        with open(airport_yaml, 'w') as f:
            f.write(yaml_text)

    analysis_out = airport_dir / f"{date_compact}_{icao}.out"
    csv_out = airport_dir / f"{date_compact}_{icao}.csv.out"

    print(f"📊 Running analysis for {icao} ({date_compact})...")
    cmd = (f"python3 src/analyzers/prox_analyze_from_files.py "
           f"--yaml {airport_yaml} "
           f"--resample --sorted-file {trace_gz} --animate-los "
           f"--animation-dir {airport_dir} "
           f"> {analysis_out} 2>&1")
    returncode = run_command(cmd, dry_run=dry_run)

    if not dry_run and returncode == 0:
        # Extract point count from "Finished streaming X points" line
        result = subprocess.run(
            f"tail -10 {analysis_out} | grep 'Finished streaming'",
            shell=True, capture_output=True, text=True)
        if result.stdout:
            print(f"  {result.stdout.strip()}")

        # grep returns 1 when no matches - no LOS events is normal
        ret = run_command(f"grep CSV {analysis_out} > {csv_out}", quiet_fail=True)
        if ret != 0:
            print(f"  ⚠️ No LOS events detected for {icao} on {date_compact}")
        else:
            # Count LOS events
            with open(csv_out, 'r') as f:
                event_count = sum(1 for _ in f)
            print(f"  ✅ {event_count} LOS event(s) detected for {icao} on {date_compact}")

    return returncode


def aggregate_airport_results(icao: str, airport_info: dict,
                              dry_run: bool = False):
    """Cross-date aggregation: combine CSV outputs and generate visualization."""
    lat, lon, _ = airport_info[icao]
    airport_dir = BASE_DIR / icao
    combined_csv = airport_dir / f"{icao}_combined.csv.out"

    # Match date-prefixed files (MMDDYY_ICAO.csv.out), exclude combined file
    csv_files = sorted(f for f in airport_dir.glob("*_*.csv.out")
                       if f != combined_csv)

    if not csv_files:
        print(f"  No CSV results for {icao}")
        return

    csv_list = " ".join(str(f) for f in csv_files)

    if dry_run:
        print(f"[DRY-RUN] cat {csv_list} > {combined_csv}")
        print(f"[DRY-RUN] cat {combined_csv} | python3 src/postprocessing/visualizer.py ...")
        return

    run_command(f"cat {csv_list} > {combined_csv}")

    sw_lat, sw_lon, ne_lat, ne_lon = compute_bounds(lat, lon, ANALYSIS_RADIUS_NM)
    vis_output = airport_dir / f"{icao}_map.html"
    run_command(
        f"cat {combined_csv} | python3 src/postprocessing/visualizer.py "
        f"--sw {sw_lat},{sw_lon} --ne {ne_lat},{ne_lon} "
        f"--output {vis_output} --no-browser")


def process_single_date(date: datetime, icao_codes: list[str], airport_info: dict,
                       timer: SimpleTimer, failed_runs: list,
                       skip_download: bool, force_download: bool, dry_run: bool) -> bool:
    """Process download, shard, and analysis for a single date.

    Returns:
        True if processing completed successfully, False if critical error occurred
    """
    date_compact = date.strftime('%m%d%y')
    date_iso = date.strftime('%Y.%m.%d')

    print(f"\n{'=' * 60}")
    print(f"Processing date: {date.strftime('%m/%d/%y')} ({date.strftime('%A')})")
    print(f"{'=' * 60}")

    if not skip_download:
        # --- Download ---
        timer.start('download')
        if not dry_run:
            if not download_tar_parts(date, force=force_download):
                timer.end('download')
                print(f"❌ Download failed for {date.strftime('%m/%d/%y')}, "
                      f"skipping date.")
                for icao in icao_codes:
                    failed_runs.append((date.strftime('%m/%d/%y'), icao,
                                        "download"))
                return False
        else:
            file_prefix = f"v{date_iso}-planes-readsb-prod-0"
            for ext in ['aa', 'ab']:
                print(f"[DRY-RUN] Download {file_prefix}.tar.{ext}")
        timer.end('download')

        # --- Extract ---
        timer.start('extract')
        # Check network storage for cached file (authoritative source)
        network_global_gz = DATA_DIR / f"global_{date_compact}.gz"
        global_gz = network_global_gz

        if not dry_run:
            if not network_global_gz.exists():
                if not extract_traces(date):
                    timer.end('extract')
                    print(f"❌ Extraction failed for "
                          f"{date.strftime('%m/%d/%y')}, skipping date.")
                    for icao in icao_codes:
                        failed_runs.append((date.strftime('%m/%d/%y'),
                                            icao, "extract"))
                    return False
            else:
                print(f"✅ {network_global_gz.name} exists in network storage. "
                      f"Skipping extraction and conversion.")
        else:
            print(f"[DRY-RUN] Extract traces")
        timer.end('extract')

        # --- Convert traces (global, no spatial filter) ---
        timer.start('convert_traces')
        if not dry_run:
            if not network_global_gz.exists():
                try:
                    # Returns local /tmp path for immediate use
                    global_gz = convert_traces_global(date)
                except RuntimeError as e:
                    timer.end('convert_traces')
                    print(f"❌ {e}, skipping date.")
                    for icao in icao_codes:
                        failed_runs.append((date.strftime('%m/%d/%y'),
                                            icao, "convert"))
                    return False

                # Clean traces after conversion
                cleanup_traces()
        else:
            print(f"[DRY-RUN] Convert traces -> {global_gz}")
        timer.end('convert_traces')

        # --- Pass 1: Shard ---
        # Generate multi-airport shard YAML
        shard_yaml_text = generate_multi_airport_yaml(
            icao_codes, date_compact, airport_info)
        shard_yaml_path = DATA_DIR / f"shard_{date_compact}.yaml"

        if not dry_run:
            # Ensure output directories exist
            for icao in icao_codes:
                (BASE_DIR / icao).mkdir(parents=True, exist_ok=True)

            with open(shard_yaml_path, 'w') as f:
                f.write(shard_yaml_text)

        print(f"🔍 Pass 1: Sharding {global_gz.name} -> "
              f"{len(icao_codes)} airports...")
        timer.start('shard_pass')
        shard_result = run_shard_pass(global_gz, shard_yaml_path,
                                      dry_run=dry_run)
        timer.end('shard_pass')

        # Clean up local temp file after successful shard
        if shard_result == 0 and not dry_run and str(global_gz).startswith('/tmp/'):
            print(f"🧹 Cleaning up local temp file {global_gz.name}...")
            try:
                global_gz.unlink()
            except Exception as e:
                print(f"⚠️ Could not delete temp file {global_gz}: {e}")

        if shard_result != 0 and not dry_run:
            print(f"❌ Shard pass failed for {date.strftime('%m/%d/%y')}")
            for icao in icao_codes:
                failed_runs.append((date.strftime('%m/%d/%y'), icao,
                                    "shard"))
            return False

    # --- Pass 2: Per-airport analysis ---
    print(f"\n📊 Pass 2: Per-airport LOS analysis...")
    for icao in icao_codes:
        timer.start(f'analyze_{icao}')
        returncode = run_airport_analysis(
            icao, date_compact, airport_info, dry_run=dry_run)
        timer.end(f'analyze_{icao}')
        if returncode != 0:
            failed_runs.append((date.strftime('%m/%d/%y'), icao,
                                "analysis"))

    return True


def run_aggregation_phase(icao_codes: list[str], airport_info: dict,
                         timer: SimpleTimer, dry_run: bool = False):
    """Run cross-date aggregation for all airports.

    Args:
        icao_codes: List of airport ICAO codes
        airport_info: Dict mapping ICAO -> (lat, lon, field_alt)
        timer: Timer instance for tracking
        dry_run: Whether this is a dry run
    """
    print(f"\n{'=' * 60}")
    print(f"Cross-date aggregation")
    print(f"{'=' * 60}")

    timer.start('aggregation')
    for icao in icao_codes:
        print(f"\n  Aggregating results for {icao}...")
        aggregate_airport_results(icao, airport_info, dry_run=dry_run)
    timer.end('aggregation')


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

  # Re-run analysis only (skip download/extract/convert/shard):
  python src/tools/batch_los_pipeline.py \\
      --start-date 01/15/26 --end-date 01/15/26 \\
      --airports examples/busiest_nontowered.txt \\
      --max-airports 3 --skip-download
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
    parser.add_argument("--skip-download", action="store_true",
                        help="Skip download/extract/convert/shard, "
                             "re-run analysis on existing sharded files")
    parser.add_argument("--force-download", action="store_true",
                        help="Force re-download of raw tarballs")
    parser.add_argument("--aggregate-only", action="store_true",
                        help="Skip all processing, only run cross-date aggregation")
    parser.add_argument("--timing-output", type=str, default="timing_report.json",
                        help="Path for timing report JSON file (default: timing_report.json)")

    args = parser.parse_args()

    if args.end_date < args.start_date:
        parser.error("End date must be >= start date")

    # Load airports and convert to ICAO
    faa_codes = load_airport_list(args.airports, args.max_airports)
    icao_codes = [faa_to_icao(code) for code in faa_codes]

    # Generate date range
    dates = generate_date_range(args.start_date, args.end_date, args.day_filter)

    if not dates:
        print(f"No dates match filter '{args.day_filter}' in range "
              f"{args.start_date.strftime('%m/%d/%y')} to "
              f"{args.end_date.strftime('%m/%d/%y')}")
        return

    # Load airport info (lat/lon/elevation) for all airports
    print("Loading airport data...")
    airport_info = {}
    for icao in icao_codes:
        try:
            airport_info[icao] = load_airport_info(icao)
        except (ValueError, TypeError) as e:
            print(f"⚠️ Could not load {icao}: {e}")

    # Remove airports we couldn't load
    icao_codes = [c for c in icao_codes if c in airport_info]
    if not icao_codes:
        print("No valid airports found.")
        return

    # Estimate download size
    estimated_gb, cached_dates, uncached_dates = estimate_download_size(
        dates, DATA_DIR, ESTIMATED_DAILY_DATA_GB)

    # Summary
    print_pipeline_summary(
        dates, cached_dates, uncached_dates, icao_codes,
        estimated_gb, ESTIMATED_DAILY_DATA_GB, args.day_filter,
        skip_download=args.skip_download,
        dry_run=args.dry_run,
        aggregate_only=args.aggregate_only
    )

    # Main processing loop
    failed_runs = []
    timer = SimpleTimer()
    timer.start('total_pipeline')

    if not args.aggregate_only:
        # Process all dates
        for date in dates:
            process_single_date(
                date, icao_codes, airport_info, timer, failed_runs,
                skip_download=args.skip_download,
                force_download=args.force_download,
                dry_run=args.dry_run
            )

    # --- Cross-date aggregation ---
    run_aggregation_phase(icao_codes, airport_info, timer, dry_run=args.dry_run)

    # Summary
    timer.end('total_pipeline')
    print_completion_summary(dates, icao_codes, failed_runs)

    # Save timing report
    if not args.dry_run:
        timer.save_report(args.timing_output)


if __name__ == "__main__":
    main()
