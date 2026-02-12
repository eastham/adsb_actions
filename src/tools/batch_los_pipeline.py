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
        --start-date 01/01/25 --end-date 01/01/26 \\
        --airports examples/busiest_nontowered_and_local.txt \\
        --max-airports 100

    # one airport, skip data that we don't already have downloaded/sharded:
    python src/tools/batch_los_pipeline.py \\
        --start-date 06/01/25 --end-date 06/05/26 \\
        --airports KWVI --analysis-only
"""

import argparse
import multiprocessing
import os
import re
import shutil
import signal
import subprocess
import sys
import time
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
    validate_date,
    estimate_download_size,
    print_pipeline_summary,
    print_completion_summary,
    FT_MAX_ABOVE_AIRPORT,
    FT_MIN_BELOW_AIRPORT,
    ANALYSIS_RADIUS_NM,
)

# Approximate size of daily ADS-B data from adsb.lol (two tar parts combined)
ESTIMATED_DAILY_DATA_GB = 3.0
DATA_DIR = Path("data")
BASE_DIR = Path("examples/generated")
GZ_DATA_PREFIX = "global_"  # Prefix for global sorted JSONL input files from convert_traces.py

# Track incomplete files for cleanup on interruption
_incomplete_files = set()
_cleanup_registered = False


def register_cleanup_handler():
    """Register signal handler for cleanup on Ctrl-C."""
    global _cleanup_registered
    if _cleanup_registered:
        return

    def cleanup_handler(_signum, _frame):
        """Clean up incomplete files on interruption."""
        if _incomplete_files:
            print("\n\n🧹 Interrupted! Cleaning up incomplete files...")
            for filepath in _incomplete_files:
                if filepath.exists():
                    try:
                        filepath.unlink()
                        print(f"  Deleted: {filepath}")
                    except Exception as e:
                        print(f"  Failed to delete {filepath}: {e}")
        sys.exit(1)

    signal.signal(signal.SIGINT, cleanup_handler)
    signal.signal(signal.SIGTERM, cleanup_handler)
    _cleanup_registered = True


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
        print(f"Generating multi-airport yaml, data target dir: {output_path}")

        lines.append(f"  {icao}_shard:")
        lines.append(f"    conditions:")
        lines.append(f"      latlongring: [{ANALYSIS_RADIUS_NM}, {lat}, {lon}]")
        lines.append(f"    actions:")
        lines.append(f"      emit_jsonl: {output_path}")

    return "\n".join(lines) + "\n"


def download_tar_parts(date_obj: datetime, data_dir: Path = None, force: bool = False) -> bool:
    """Download the two tar archive parts for a date, from the
    adsb.lol GitHub releases.

    Args:
        date_obj: Date to download data for
        data_dir: Directory to save downloaded files (defaults to DATA_DIR)
        force: Force re-download even if file exists

    Returns True if both parts are available (downloaded or cached).
    """
    if data_dir is None:
        data_dir = DATA_DIR
    else:
        data_dir = Path(data_dir)

    date_iso = date_obj.strftime('%Y.%m.%d')
    full_year = date_obj.strftime('%Y')
    file_prefix = f"v{date_iso}-planes-readsb-prod-0"

    data_dir.mkdir(parents=True, exist_ok=True)

    for ext in ['aa', 'ab']:
        local_file = data_dir / f"{file_prefix}.tar.{ext}"
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
            total_size = int(r.headers.get('content-length', 0))
            downloaded = 0
            start_time = time.time()

            with open(local_file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=16*1024):
                    f.write(chunk)
                    downloaded += len(chunk)

                    # Print progress every 50MB
                    if downloaded % (50 * 1024 * 1024) < 8192:
                        elapsed = time.time() - start_time
                        if elapsed > 0:
                            speed_mbps = (downloaded / elapsed) / (1024 * 1024)
                            if total_size > 0:
                                pct = (downloaded / total_size) * 100
                                print(f"  {pct:.1f}% - {downloaded / (1024*1024):.1f} MB @ {speed_mbps:.1f} MB/s")
                            else:
                                print(f"  {downloaded / (1024*1024):.1f} MB @ {speed_mbps:.1f} MB/s")
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
    # Extract only the traces/ directory (excludes License*.txt and other files)
    result = run_command(
        f"cat {archive_pattern} | tar --options read_concatenated_archives -xf - -C {extract_base} traces/")
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
    local_temp = Path("/tmp") / f"{GZ_DATA_PREFIX}{date_compact}.gz"
    network_final = DATA_DIR / f"{GZ_DATA_PREFIX}{date_compact}.gz"

    print(f"⚙️ Converting traces to {local_temp} (local temp)...")
    traces_dir = BASE_DIR / "traces"
    result = run_command(
        f"python src/tools/convert_traces.py {traces_dir} -o {local_temp} --progress 100")
    if result != 0:
        raise RuntimeError(f"convert_traces.py failed for {date_compact}")

    # Background copy to network storage (don't block pipeline)
    print(f"📦 Copying {local_temp.name} to network storage in background...")
    run_command(
        f"mv '{local_temp}' '{network_final}' && echo '✅ Network copy complete: {network_final.name}' &",
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
    traffic_samples = airport_dir / f"{date_compact}_{icao}_traffic.csv"

    # Track output files as incomplete (for cleanup on Ctrl-C)
    if not dry_run:
        _incomplete_files.add(analysis_out)
        _incomplete_files.add(csv_out)
        _incomplete_files.add(traffic_samples)

    print(f"📊 Running analysis for {icao} ({date_compact})...")
    cmd = (f"python3 src/analyzers/prox_analyze_from_files.py "
           f"--yaml {airport_yaml} "
           f"--resample --sorted-file {trace_gz} --animate-los "
           f"--animation-dir {airport_dir} "
           # f"--export-traffic-samples {traffic_samples} "
           f"> {analysis_out} 2>&1")
    returncode = run_command(cmd, dry_run=dry_run)

    # Mark output files as complete
    if not dry_run:
        _incomplete_files.discard(analysis_out)
        _incomplete_files.discard(csv_out)
        _incomplete_files.discard(traffic_samples)

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
    lat, lon, field_elev = airport_info[icao]
    airport_dir = BASE_DIR / icao
    combined_csv = airport_dir / f"{icao}_combined.csv.out"
    
    # Match date-prefixed files (MMDDYY_ICAO.csv.out), exclude combined file
    csv_files = sorted(f for f in airport_dir.glob("*_*.csv.out")
                       if f != combined_csv)

    if not csv_files:
        print(f"  No CSV results for {icao}")
        return

    if dry_run:
        print(f"[DRY-RUN] Combine {len(csv_files)} CSV files > {combined_csv}")
        print(f"[DRY-RUN] cat {combined_csv} | python3 src/postprocessing/visualizer.py ...")
        return

    # Write metadata header, then append all CSV files
    with open(combined_csv, 'w') as outf:
        outf.write(f"# analysis_radius_nm = {ANALYSIS_RADIUS_NM}\n")
        outf.write(f"# center_lat = {lat}\n")
        outf.write(f"# center_lon = {lon}\n")

        # Concatenate all CSV files
        for csv_file in csv_files:
            with open(csv_file, 'r') as inf:
                outf.write(inf.read())
    
    # Combine traffic samples from all dates - DISABLED
    #combined_traffic = airport_dir / f"{icao}_traffic_combined.csv"
    #traffic_files = sorted(airport_dir.glob("*_*_traffic.csv"))
    combined_traffic = traffic_files = None  # Disable traffic sample combination for now

    if traffic_files:
        # First combine all files
        temp_combined = airport_dir / f"{icao}_traffic_temp.csv"
        traffic_list = " ".join(str(f) for f in traffic_files)
        run_command(f"cat {traffic_list} > {temp_combined}")

        # Check if we need to downsample the combined file
        result = subprocess.run(f"wc -l < {temp_combined}", shell=True, capture_output=True, text=True)
        line_count = int(result.stdout.strip())
        max_combined_samples = 20000  # Limit for multi-day visualization (~8MB HTML)

        if line_count > max_combined_samples:
            # Downsample: take every Nth line
            sample_rate = max(1, line_count // max_combined_samples)
            run_command(f"awk 'NR % {sample_rate} == 0' {temp_combined} > {combined_traffic}")
            run_command(f"rm {temp_combined}")
            print(f"  Combined {len(traffic_files)} traffic files: {line_count:,} points "
                  f"→ downsampled to ~{max_combined_samples:,} (every {sample_rate}th point)")
        else:
            run_command(f"mv {temp_combined} {combined_traffic}")
            print(f"  Combined {len(traffic_files)} traffic files: {line_count:,} points")
    else:
        combined_traffic = None

    # Build busyness data (traffic counts + METAR weather categories)
    busyness_json = None
    try:
        from busyness import build_busyness_data
        busyness_data = build_busyness_data(icao, airport_dir,
                                            metar_cache_dir=airport_dir,
                                            field_elev=field_elev)
        if busyness_data:
            import json
            busyness_json = airport_dir / f"{icao}_busyness.json"
            busyness_json.write_text(json.dumps(busyness_data))
            print(f"  Busyness data: {busyness_data['numDates']} dates, "
                  f"max={busyness_data['globalMax']} aircraft/hr")
    except Exception as e:
        print(f"  Warning: busyness data generation failed: {e}")

    # Build data quality assessment
    quality_json = None
    try:
        from data_quality import build_data_quality
        quality_data = build_data_quality(icao, airport_dir,
                                          field_elev=field_elev,
                                          airport_lat=lat,
                                          airport_lon=lon)
        if quality_data:
            import json
            quality_json = airport_dir / f"{icao}_quality.json"
            quality_json.write_text(json.dumps(quality_data))
            lost = quality_data['lostRate']
            lost_str = f"{lost:.0%}" if lost is not None else "N/A"
            gap = quality_data['medianGapS']
            gap_str = f"{gap:.1f}s" if gap is not None else "N/A"
            print(f"  Data quality: {quality_data['score']} "
                  f"(lost={lost_str}, gap={gap_str})")
    except Exception as e:
        print(f"  Warning: data quality assessment failed: {e}")

    sw_lat, sw_lon, ne_lat, ne_lon = compute_bounds(lat, lon, ANALYSIS_RADIUS_NM)
    vis_output = airport_dir / f"{icao}_map.html"

    # Build visualizer command with optional traffic samples
    vis_cmd = (
        f"cat {combined_csv} | python3 src/postprocessing/visualizer.py "
        f"--sw {sw_lat},{sw_lon} --ne {ne_lat},{ne_lon} "
        f"--native-heatmap --heatmap-opacity 0.5 --heatmap-radius 25 "
    )

    if combined_traffic and combined_traffic.exists():
        vis_cmd += f"--traffic-samples {combined_traffic} "
        print(f"  Using traffic samples: {combined_traffic.name}")

    if busyness_json and busyness_json.exists():
        vis_cmd += f"--busyness-data {busyness_json} "

    if quality_json and quality_json.exists():
        vis_cmd += f"--data-quality {quality_json} "

    vis_cmd += "--traffic-tiles tiles/traffic/ "
    vis_cmd += f"--output {vis_output} --no-browser "

    vis_cmd += "--heatmap-label '6/1/25 - 6/15/25' "  # TODO dynamic label
    vis_cmd += "--traffic-label '6/1/25 - 8/31/25' "  # TODO dynamic label

    run_command(vis_cmd)


def process_single_date(date: datetime, icao_codes: list[str], airport_info: dict,
                       timer: SimpleTimer, failed_runs: list,
                       analysis_only: bool, force_download: bool, dry_run: bool) -> bool:
    """Process download, shard, and analysis for a single date.

    Returns:
        True if processing completed successfully, False if critical error occurred
    """
    date_compact = date.strftime('%m%d%y')
    date_iso = date.strftime('%Y.%m.%d')

    print(f"\n{'=' * 60}")
    print(f"Processing date: {date.strftime('%m/%d/%y')} ({date.strftime('%A')})")
    print(f"{'=' * 60}")

    if not analysis_only:
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
        network_global_gz = DATA_DIR / f"{GZ_DATA_PREFIX}{date_compact}.gz"
        global_gz = network_global_gz

        print(f"🔍 Checking for existing JSONL file in network storage: {network_global_gz}...")
        if not dry_run:
            if not network_global_gz.exists():
                print(f" extracting to {network_global_gz}...")
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

        # --- Pass 1: Shard into per-airport JSONL gzip files ---
        # Check if all airport shard files already exist
        all_shards_exist = True
        if not dry_run:
            for icao in icao_codes:
                airport_gz = BASE_DIR / icao / f"{date_compact}_{icao}.gz"
                if not airport_gz.exists() or airport_gz.stat().st_size < 30:
                    all_shards_exist = False
                    break

        if all_shards_exist and analysis_only:
            print(f"✅ All {len(icao_codes)} airport shard files already exist, skipping shard pass.")
            timer.start('shard_pass')  # For timing consistency
            timer.end('shard_pass')
            shard_result = 0
        else:
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

                # Track shard files as incomplete (for cleanup on Ctrl-C)
                for icao in icao_codes:
                    shard_file = BASE_DIR / icao / f"{date_compact}_{icao}.gz"
                    _incomplete_files.add(shard_file)

            print(f"🔍 Pass 1: Sharding {global_gz.name} -> "
                  f"{len(icao_codes)} airports...")
            timer.start('shard_pass')
            shard_result = run_shard_pass(global_gz, shard_yaml_path,
                                          dry_run=dry_run)
            timer.end('shard_pass')

            # Mark shard files as complete
            if shard_result == 0 and not dry_run:
                for icao in icao_codes:
                    shard_file = BASE_DIR / icao / f"{date_compact}_{icao}.gz"
                    _incomplete_files.discard(shard_file)

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

    # Parallel execution for analysis phase (each airport is independent)
    if len(icao_codes) > 1 and not dry_run:
        print(f"  Running {len(icao_codes)} analyses in parallel...")

        # Prepare arguments for worker processes
        worker_args = [(icao, date_compact, airport_info, dry_run) for icao in icao_codes]

        # Use all available CPUs
        num_workers = min(multiprocessing.cpu_count(), len(icao_codes))
        print(f"  Using {num_workers} worker processes")

        analysis_start = time.time()
        with multiprocessing.Pool(processes=num_workers) as pool:
            # Process airports in parallel
            for icao, returncode in pool.imap_unordered(_analyze_airport_worker, worker_args):
                # Track timing for each airport (approximate, since they run in parallel)
                timer.start(f'analyze_{icao}')
                timer.end(f'analyze_{icao}')

                if returncode == 0:
                    print(f"  ✓ Completed {icao}")
                else:
                    print(f"  ✗ Failed {icao}")
                    failed_runs.append((date.strftime('%m/%d/%y'), icao, "analysis"))

        analysis_elapsed = time.time() - analysis_start
        print(f"  Parallel analysis completed in {analysis_elapsed:.1f}s "
              f"(~{analysis_elapsed/len(icao_codes):.1f}s per airport)")
    else:
        # Sequential execution (for single airport or dry-run)
        for icao in icao_codes:
            timer.start(f'analyze_{icao}')
            returncode = run_airport_analysis(
                icao, date_compact, airport_info, dry_run=dry_run)
            timer.end(f'analyze_{icao}')
            if returncode != 0:
                failed_runs.append((date.strftime('%m/%d/%y'), icao,
                                    "analysis"))

    return True


def _analyze_airport_worker(args):
    """Worker function for parallel analysis (must be top-level for pickling)."""
    icao, date_compact, airport_info, dry_run = args
    returncode = run_airport_analysis(icao, date_compact, airport_info, dry_run=dry_run)
    return icao, returncode


def _aggregate_airport_worker(args):
    """Worker function for parallel aggregation (must be top-level for pickling)."""
    icao, airport_info, dry_run = args
    aggregate_airport_results(icao, airport_info, dry_run=dry_run)
    return icao


def run_aggregation_phase(icao_codes: list[str], airport_info: dict,
                         timer: SimpleTimer, dry_run: bool = False, parallel: bool = True) -> dict:
    """Run cross-date aggregation for all airports.

    Args:
        icao_codes: List of airport ICAO codes
        airport_info: Dict mapping ICAO -> (lat, lon, field_alt)
        timer: Timer instance for tracking
        dry_run: Whether this is a dry run
        parallel: Whether to run aggregations in parallel (default: True)

    Returns:
        Dict mapping ICAO to output stats (num_events, data_quality, html_path)
    """
    print(f"\n{'=' * 60}")
    print(f"Cross-date aggregation")
    print(f"{'=' * 60}")

    timer.start('aggregation')

    if parallel and not dry_run and len(icao_codes) > 1:
        # Parallel execution - much faster for visualization generation
        print(f"  Running {len(icao_codes)} aggregations in parallel...")

        # Prepare arguments for worker processes
        worker_args = [(icao, airport_info, dry_run) for icao in icao_codes]

        # Use all available CPUs
        num_workers = min(multiprocessing.cpu_count(), len(icao_codes))
        print(f"  Using {num_workers} worker processes")

        with multiprocessing.Pool(processes=num_workers) as pool:
            # Process airports in parallel, show progress as they complete
            for icao in pool.imap_unordered(_aggregate_airport_worker, worker_args):
                print(f"  ✓ Completed {icao}")
    else:
        # Sequential execution (for dry-run or single airport)
        for icao in icao_codes:
            print(f"\n  Aggregating results for {icao}...")
            aggregate_airport_results(icao, airport_info, dry_run=dry_run)

    timer.end('aggregation')

    # Collect output statistics from generated files
    output_stats = {}
    if not dry_run:
        import json
        for icao in icao_codes:
            airport_dir = BASE_DIR / icao
            html_path = airport_dir / f"{icao}_map.html"
            combined_csv = airport_dir / f"{icao}_combined.csv.out"
            quality_json = airport_dir / f"{icao}_quality.json"

            if html_path.exists():
                # Count LOS events from combined CSV
                num_events = 0
                if combined_csv.exists():
                    with open(combined_csv, 'r') as f:
                        num_events = sum(1 for _ in f)

                # Load data quality metrics
                quality_data = None
                if quality_json.exists():
                    try:
                        quality_data = json.loads(quality_json.read_text())
                    except Exception:
                        pass

                output_stats[icao] = {
                    'html_path': html_path,
                    'num_events': num_events,
                    'quality_data': quality_data
                }

    return output_stats


def print_visualization_summary(output_stats: dict):
    """Print summary of generated HTML visualizations with stats.

    Args:
        output_stats: Dict mapping ICAO to {html_path, num_events, quality_data}
    """
    if not output_stats:
        return

    print("\n" + "=" * 80)
    print("GENERATED VISUALIZATIONS")
    print("=" * 80)

    for icao in sorted(output_stats.keys()):
        stats = output_stats[icao]
        html_path = stats['html_path']
        num_events = stats['num_events']
        quality_data = stats.get('quality_data')

        print(f"\n{icao}: {html_path}")
        print(f"  LOS Events: {num_events}", end="")

        if quality_data:
            score = quality_data.get('score', 'N/A')
            completion = (quality_data.get('completionRate') or 0) * 100
            median_gap = quality_data.get('medianGapS') or 0

            # Color-code the score
            if score == 'green':
                score_display = '🟢 Green (Excellent)'
            elif score == 'yellow':
                score_display = '🟡 Yellow (Good)'
            elif score == 'red':
                score_display = '🔴 Red (Poor)'
            else:
                score_display = score

            print(f"  Data Quality: {score_display}")
            print(f"  Completion Rate: {completion:.1f}%", end="")
            print(f"  Median Gap: {median_gap:.1f}s")
        else:
            print(f"  Data Quality: No quality data available")

    print("\n" + "=" * 80)


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
      --max-airports 3 --analysis-only
"""
    )

    parser.add_argument("--start-date", type=validate_date, required=True,
                        help="Start date in mm/dd/yy format")
    parser.add_argument("--end-date", type=validate_date, required=True,
                        help="End date in mm/dd/yy format")
    parser.add_argument("--airports", type=str, required=True,
                        help="Airport code (e.g. WVI) or path to file with codes (one per line)")
    parser.add_argument("--max-airports", type=int, default=None,
                        help="Limit to first N airports from the list")
    parser.add_argument("--day-filter", type=str, default="all",
                        choices=["all", "weekday", "weekend"],
                        help="Filter dates by day type (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print commands without executing")
    parser.add_argument("--analysis-only", action="store_true",
                        help="Skip download/extract/convert/shard preprocessing, "
                             "re-run analysis on existing sharded files")
    parser.add_argument("--force-download", action="store_true",
                        help="Force re-download of raw tarballs")
    parser.add_argument("--aggregate-only", action="store_true",
                        help="Skip all processing, only run cross-date aggregation")
    parser.add_argument("--timing-output", type=str,
                        help="Path for timing report JSON file (default: timing_report.json)")

    args = parser.parse_args()

    # Register cleanup handler for Ctrl-C
    register_cleanup_handler()

    if args.end_date < args.start_date:
        parser.error("End date must be >= start date")

    # Load airports and convert to ICAO
    if os.path.isfile(args.airports):
        faa_codes = load_airport_list(args.airports, args.max_airports)
    else:
        faa_codes = [args.airports]
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
        analysis_only=args.analysis_only,
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
                analysis_only=args.analysis_only,
                force_download=args.force_download,
                dry_run=args.dry_run
            )

    # --- Cross-date aggregation ---
    output_stats = run_aggregation_phase(icao_codes, airport_info, timer, dry_run=args.dry_run)

    # Summary
    timer.end('total_pipeline')
    print_completion_summary(dates, icao_codes, failed_runs)

    # Print visualization summary with stats
    if output_stats and not args.dry_run:
        print_visualization_summary(output_stats)

    # Save timing report
    if not args.dry_run and args.timing_output:
        timer.save_report(args.timing_output)


if __name__ == "__main__":
    main()
