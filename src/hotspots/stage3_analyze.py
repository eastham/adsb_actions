#!/usr/bin/env python3
"""
Stage 3: LOS Analysis

For each grid cell shard produced by Stage 2, runs LOS detection and writes:
  - data/v2/events/{YYYYMMDD}_{lat}_{lon}.parquet   — columnar event records
  - data/v2/events/{YYYYMMDD}_{lat}_{lon}.csv        — same, as clean CSV
  - data/v2/animations/{YYYYMMDD}_{lat}_{lon}/       — sampled animation HTMLs

Cells can be run in parallel via --workers.

Usage:
    # Single cell:
    python src/hotspots/stage3_analyze.py \
        --shard data/v2/grid/20260101_37_-122.gz

    # All shards for a date:
    python src/hotspots/stage3_analyze.py \
        --date 20260101 --workers 4

    # Explicit shard dir:
    python src/hotspots/stage3_analyze.py \
        --shard-dir data/v2/grid --date 20260101 --workers 8
"""

import argparse
import multiprocessing
import os
import sys
import time
from pathlib import Path

# Allow running from project root
_ROOT = Path(__file__).resolve().parents[2]
for _p in [str(_ROOT / "src"), str(_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

V2_DATA_ROOT = Path("data/v2")
GRID_DIR = V2_DATA_ROOT / "grid"
EVENTS_DIR = V2_DATA_ROOT / "events"
ANIMATIONS_DIR = V2_DATA_ROOT / "animations"


def shard_stem(shard_path: Path) -> str:
    """Return stem without .gz, e.g. '20260101_37_-122'."""
    name = shard_path.name
    if name.endswith(".gz"):
        name = name[:-3]
    return name


def analyze_cell(args_tuple):
    """
    Worker function: analyze a single grid cell shard.
    args_tuple = (shard_path_str, animate, skip_existing)
    Returns (stem, event_count, elapsed_s, error_msg_or_None)
    """
    shard_path_str, animate, skip_existing = args_tuple
    shard_path = Path(shard_path_str)
    stem = shard_stem(shard_path)

    date_tag = stem.split("_")[0]
    parquet_path = str(EVENTS_DIR / date_tag / f"{stem}.parquet")
    csv_path = str(EVENTS_DIR / date_tag / f"{stem}.csv")
    animation_dir = str(ANIMATIONS_DIR / stem)

    sentinel_path = parquet_path.replace(".parquet", ".empty")
    if skip_existing and (os.path.exists(parquet_path) or os.path.exists(sentinel_path)):
        return (stem, None, 0, "skipped")

    t0 = time.time()
    try:
        from hotspots.los_detector import LOSDetector
        detector = LOSDetector(animate=animate,
                               animation_dir=animation_dir if animate else None)
        # Parse cell bounds from filename (e.g. 20260101_37_-122)
        parts = stem.split("_")
        try:
            lat_min = int(parts[1])
            lon_min = int(parts[2])
        except (IndexError, ValueError):
            lat_min = lon_min = None
        lat_max = lat_min + 1 if lat_min is not None else None
        lon_max = lon_min + 1 if lon_min is not None else None
        n_events = detector.run(str(shard_path),
                                lat_min=lat_min, lat_max=lat_max,
                                lon_min=lon_min, lon_max=lon_max)
        detector.write_parquet(parquet_path)
        detector.write_csv(csv_path)
        if n_events == 0:
            detector.write_empty_sentinel(parquet_path)
        elapsed = time.time() - t0
        return (stem, n_events, elapsed, None)
    except Exception as e:
        import traceback
        elapsed = time.time() - t0
        return (stem, None, elapsed, traceback.format_exc())


def find_shards(shard_dir: Path, date: str) -> list:
    """Return list of shard paths matching the given date prefix."""
    return sorted((shard_dir / date).glob(f"{date}_*.gz"))


def analyze_shards(shards: list, workers: int = 1, animate: bool = False,
                   skip_existing: bool = False) -> dict:
    """
    Run LOS analysis on a list of shard paths.

    Returns dict: {stem: {'events': N, 'elapsed_s': T, 'error': str_or_None}}
    """
    # Date subdirs are created per-cell in analyze_cell() via write_parquet/write_csv
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)

    # Sort largest shards first (LPT heuristic) so big cells don't become tail blockers.
    args_list = [(str(s), animate, skip_existing)
                 for s in sorted(shards, key=lambda p: p.stat().st_size, reverse=True)]
    results = {}

    if workers > 1:
        with multiprocessing.Pool(processes=workers) as pool:
            for stem, n_events, elapsed, error in pool.imap_unordered(analyze_cell, args_list):
                results[stem] = {"events": n_events, "elapsed_s": elapsed, "error": error}
                _print_result(stem, n_events, elapsed, error)
    else:
        for args in args_list:
            stem, n_events, elapsed, error = analyze_cell(args)
            results[stem] = {"events": n_events, "elapsed_s": elapsed, "error": error}
            _print_result(stem, n_events, elapsed, error)

    return results


def _print_result(stem, n_events, elapsed, error):
    if error == "skipped":
        print(f"  [skip] {stem} already exists")
    elif error:
        print(f"  [ERROR] {stem} failed in {elapsed:.1f}s:")
        # Print first line of traceback only to keep output clean
        first_line = error.strip().split("\n")[-1]
        print(f"         {first_line}")
    else:
        print(f"  [done] {stem}: {n_events} events in {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(
        description="Stage 3: Run LOS analysis on grid cell shards.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--shard", help="Single shard file to analyze")
    group.add_argument("--date", help="Date tag (YYYYMMDD) to analyze all shards for that date")
    parser.add_argument("--shard-dir", default=str(GRID_DIR),
                        help=f"Directory containing shards (default: {GRID_DIR})")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of parallel worker processes (default: 1)")
    parser.add_argument("--animate", action="store_true",
                        help="Generate animation HTMLs for LOS events")
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip cells whose Parquet output already exists")
    args = parser.parse_args()

    if args.shard:
        shards = [Path(args.shard)]
        if not shards[0].exists():
            print(f"ERROR: Shard not found: {args.shard}", file=sys.stderr)
            sys.exit(1)
    else:
        shard_dir = Path(args.shard_dir)
        shards = find_shards(shard_dir, args.date)
        if not shards:
            print(f"ERROR: No shards found for date {args.date} in {shard_dir}",
                  file=sys.stderr)
            sys.exit(1)

    print(f"Analyzing {len(shards)} shard(s) with {args.workers} worker(s)")
    print(f"  Events output: {EVENTS_DIR}/")
    if args.animate:
        print(f"  Animations: {ANIMATIONS_DIR}/")

    t0 = time.time()
    results = analyze_shards(shards, workers=args.workers,
                             animate=args.animate, skip_existing=args.skip_existing)
    total_elapsed = time.time() - t0

    # Summary
    done = {k: v for k, v in results.items() if v["error"] not in ("skipped", None.__class__) or v["error"] is None}
    errors = {k: v for k, v in results.items() if v["error"] and v["error"] != "skipped"}
    skipped = {k: v for k, v in results.items() if v["error"] == "skipped"}
    total_events = sum(v["events"] or 0 for v in done.values())

    print(f"\nDone in {total_elapsed:.1f}s.")
    print(f"  Cells analyzed: {len(done)}, skipped: {len(skipped)}, errors: {len(errors)}")
    print(f"  Total LOS events: {total_events:,}")

    if errors:
        print(f"\nFailed cells:")
        for stem, v in errors.items():
            print(f"  {stem}")

    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
