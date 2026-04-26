#!/usr/bin/env python3
"""
Stage 4: Aggregate per-cell Parquet files into a regional event database.

Reads all per-cell Parquet files (from Stage 3) for a date range and region,
concatenates them, deduplicates any cross-cell duplicates (shouldn't occur with
1° tiles but added as a safety net), and writes:
  - data/v2/regional/{region}_{start}_{end}.parquet

Usage:
    # Aggregate all cells for a single date:
    python src/hotspots/stage4_aggregate.py \
        --date 20260101 --region CA

    # Aggregate a date range:
    python src/hotspots/stage4_aggregate.py \
        --start 20260101 --end 20260131 --region CA

    # Aggregate specific cells (lat/lon bounding box):
    python src/hotspots/stage4_aggregate.py \
        --date 20260101 --lat-min 37 --lat-max 39 --lon-min -123 --lon-max -121

    # Aggregate ALL available cells (no region filter):
    python src/hotspots/stage4_aggregate.py --date 20260101
"""

import argparse
import os
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
for _p in [str(_ROOT / "src"), str(_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import pandas as pd

V2_DATA_ROOT = Path("data/v2")
EVENTS_DIR = V2_DATA_ROOT / "events"
REGIONAL_DIR = V2_DATA_ROOT / "regional"

# Named regions: map to lat/lon bounding boxes
REGIONS = {
    "CA":  {"lat_min": 32, "lat_max": 42, "lon_min": -124, "lon_max": -114},
    "NV":  {"lat_min": 35, "lat_max": 42, "lon_min": -120, "lon_max": -114},
    "OR":  {"lat_min": 42, "lat_max": 47, "lon_min": -124, "lon_max": -116},
    "WA":  {"lat_min": 45, "lat_max": 49, "lon_min": -125, "lon_max": -117},
    "AZ":  {"lat_min": 31, "lat_max": 37, "lon_min": -115, "lon_max": -109},
    "CONUS": {"lat_min": 24, "lat_max": 50, "lon_min": -125, "lon_max": -65},
}


def parquet_stem_to_date_cell(stem: str):
    """Parse '20260101_37_-122' into ('20260101', 37, -122)."""
    parts = stem.split("_")
    if len(parts) < 3:
        return None
    try:
        date = parts[0]
        lat = int(parts[1])
        # Handle negative longitude: '20260101_37_-122' splits to ['20260101','37','-122']
        lon = int(parts[2])
        return date, lat, lon
    except ValueError:
        return None


def find_parquet_files(events_dir: Path, dates: list, lat_min=None, lat_max=None,
                       lon_min=None, lon_max=None) -> list:
    """Return Parquet files matching date list and optional lat/lon filter."""
    dates_set = set(dates) if dates else None
    matches = []
    date_dirs = sorted(d for d in events_dir.iterdir() if d.is_dir())
    print(f"  Scanning {len(date_dirs)} date directories...", flush=True)
    for date_dir in date_dirs:
        date = date_dir.name
        if dates_set and date not in dates_set:
            continue
        for p in sorted(date_dir.glob("*.parquet")):
            parsed = parquet_stem_to_date_cell(p.stem)
            if parsed is None:
                continue
            _, lat, lon = parsed
            if lat_min is not None and not (lat_min <= lat < lat_max):
                continue
            if lon_min is not None and not (lon_min <= lon < lon_max):
                continue
            matches.append(p)
    return matches


def date_range(start: str, end: str) -> list:
    """Return list of YYYYMMDD strings from start to end (inclusive)."""
    import datetime
    fmt = "%Y%m%d"
    d = datetime.datetime.strptime(start, fmt)
    end_d = datetime.datetime.strptime(end, fmt)
    dates = []
    while d <= end_d:
        dates.append(d.strftime(fmt))
        d += datetime.timedelta(days=1)
    return dates


def aggregate(parquet_files: list) -> pd.DataFrame:
    """Load and concatenate Parquet files; deduplicate on (flight1, flight2, timestamp)."""
    if not parquet_files:
        return pd.DataFrame()

    import pyarrow.parquet as pq

    n = len(parquet_files)

    # Filter to non-empty files (empty cells write a 636-byte parquet with no schema)
    non_empty = [p for p in parquet_files if p.stat().st_size > 1000]
    skipped = n - len(non_empty)
    print(f"  Reading {len(non_empty):,} parquet files ({skipped:,} empty skipped)...", flush=True)

    if not non_empty:
        return pd.DataFrame()

    t_read = time.time()
    # Batch read via PyArrow — much faster than a Python loop of pd.read_parquet.
    # Chunk the file list so we can print progress; a single read_table() call over
    # tens of thousands of files is silent for many minutes.
    CHUNK = 1000
    try:
        tables = []
        events_so_far = 0
        for i in range(0, len(non_empty), CHUNK):
            chunk = non_empty[i:i + CHUNK]
            t_chunk = time.time()
            tbl = pq.read_table([str(p) for p in chunk])
            tables.append(tbl)
            events_so_far += tbl.num_rows
            done = min(i + CHUNK, len(non_empty))
            print(f"  {done:,}/{len(non_empty):,} ({done/len(non_empty)*100:.0f}%) — "
                  f"{events_so_far:,} events, chunk {time.time()-t_chunk:.1f}s, "
                  f"elapsed {time.time()-t_read:.1f}s", flush=True)
        import pyarrow as pa
        table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        combined = table.to_pandas()
    except Exception as e:
        # Fall back to per-file loop on schema mismatch or other error
        print(f"  [warn] Batch read failed ({e}), falling back to per-file loop", file=sys.stderr)
        dfs = []
        events_so_far = 0
        report_every = max(1, len(non_empty) // 20)
        for i, p in enumerate(non_empty):
            try:
                df = pd.read_parquet(p)
                dfs.append(df)
                events_so_far += len(df)
            except Exception as e2:
                print(f"  [warn] Could not read {p.name}: {e2}", file=sys.stderr)
            if (i + 1) % report_every == 0 or (i + 1) == len(non_empty):
                print(f"  {i+1}/{len(non_empty)} ({(i+1)/len(non_empty)*100:.0f}%) — {events_so_far:,} events",
                      flush=True)
        if not dfs:
            return pd.DataFrame()
        combined = pd.concat(dfs, ignore_index=True)

    print(f"  Read {len(combined):,} events in {time.time()-t_read:.1f}s", flush=True)

    # Safety dedup — should be rare with non-overlapping 1° tiles
    before = len(combined)
    combined = combined.drop_duplicates(subset=["flight1", "flight2", "timestamp"])
    after = len(combined)
    if before != after:
        print(f"  [info] Removed {before - after} duplicate events after aggregation")

    combined = combined.sort_values("timestamp").reset_index(drop=True)
    return combined


def main():
    parser = argparse.ArgumentParser(
        description="Stage 4: Aggregate per-cell Parquet into regional event DB.")

    # Date selection
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--date", help="Single date (YYYYMMDD)")
    date_group.add_argument("--start", help="Start date (YYYYMMDD) for date range")

    parser.add_argument("--end", help="End date (YYYYMMDD) — required with --start")

    # Region / cell selection
    region_group = parser.add_mutually_exclusive_group()
    region_group.add_argument("--region", choices=list(REGIONS.keys()),
                              help="Named region (e.g. CA, CONUS)")
    region_group.add_argument("--lat-min", type=int, help="Min latitude (inclusive)")

    parser.add_argument("--lat-max", type=int)
    parser.add_argument("--lon-min", type=int)
    parser.add_argument("--lon-max", type=int)

    parser.add_argument("--events-dir", default=str(EVENTS_DIR),
                        help=f"Events directory (default: {EVENTS_DIR})")
    parser.add_argument("--output", help="Override output Parquet path")
    args = parser.parse_args()

    # Resolve dates
    if args.date:
        dates = [args.date]
        date_label = args.date
    else:
        if not args.end:
            parser.error("--end is required when --start is used")
        dates = date_range(args.start, args.end)
        date_label = f"{args.start}_{args.end}"

    # Resolve bounding box
    lat_min = lat_max = lon_min = lon_max = None
    if args.region:
        bb = REGIONS[args.region]
        lat_min, lat_max = bb["lat_min"], bb["lat_max"]
        lon_min, lon_max = bb["lon_min"], bb["lon_max"]
        region_label = args.region
    elif args.lat_min is not None:
        lat_min, lat_max = args.lat_min, args.lat_max
        lon_min, lon_max = args.lon_min, args.lon_max
        region_label = f"{lat_min}_{lat_max}_{lon_min}_{lon_max}"
    else:
        region_label = "all"

    events_dir = Path(args.events_dir)
    REGIONAL_DIR.mkdir(parents=True, exist_ok=True)

    output_path = args.output or str(REGIONAL_DIR / f"{region_label}_{date_label}.parquet")

    print(f"Aggregating events: region={region_label}, dates={date_label}")
    print(f"  Input: {events_dir}/")
    print(f"  Output: {output_path}")

    t0 = time.time()
    parquet_files = find_parquet_files(events_dir, dates, lat_min, lat_max, lon_min, lon_max)
    print(f"  Found {len(parquet_files)} parquet file(s)")

    if not parquet_files:
        print("Nothing to aggregate.")
        return 0

    df = aggregate(parquet_files)
    elapsed = time.time() - t0

    if df.empty:
        print("No events after aggregation.")
        return 0

    df.to_parquet(output_path, index=False)

    size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"\nDone in {elapsed:.1f}s.")
    print(f"  Total events: {len(df):,}")
    print(f"  Output size: {size_mb:.1f} MB")
    if "quality" in df.columns:
        print(f"  Quality distribution: {df['quality'].value_counts().to_dict()}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
