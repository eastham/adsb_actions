#!/usr/bin/env python3
"""
Stage 2: Grid Cell Sharding

Streams a CONUS (or global) time-sorted JSONL file and writes one gzipped
shard per 1°×1° lat/lon grid cell, in a single pass.

All output goes to data/v2/grid/ — never touches v1 data/.

Usage:
    # All cells in a bounding region:
    python src/hotspots/stage2_shard.py --input data/CONUS_010126.gz \
        --lat-min 37 --lat-max 40 --lon-min -123 --lon-max -119

    # Single cell (dev/test):
    python src/hotspots/stage2_shard.py --input data/CONUS_010126.gz \
        --cell 37 -122

    # All CONUS (lat 24-50, lon -125 to -65):
    python src/hotspots/stage2_shard.py --input data/CONUS_010126.gz --conus
"""

import argparse
import gzip
import math
import os
import resource
import sys
import time
from pathlib import Path

# macOS caps open file descriptors at 256 by default even when ulimit shows
# unlimited. Raise to the hard limit so we can hold 1500+ shard handles open.
def _raise_fd_limit():
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < hard:
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))

_raise_fd_limit()

try:
    import orjson
    def _json_loads(s):
        return orjson.loads(s)
except ImportError:
    import json
    def _json_loads(s):
        return json.loads(s)

# v2 data root — all outputs go here, never to data/ directly
V2_DATA_ROOT = Path("data/v2")
GRID_DIR = V2_DATA_ROOT / "grid"

# Altitude ceiling for sharding (Class A floor)
ALT_CEILING_FT = 18_000

# CONUS bounding box (inclusive lat, exclusive at max)
CONUS_LAT_MIN, CONUS_LAT_MAX = 24, 50
CONUS_LON_MIN, CONUS_LON_MAX = -125, -65


def cell_tag(lat: int, lon: int) -> str:
    """Canonical cell identifier string, e.g. '37_-122'."""
    return f"{lat}_{lon}"


def output_path(date_tag: str, lat: int, lon: int) -> Path:
    """Return path for a shard file, organized under a date subdirectory."""
    return GRID_DIR / date_tag / f"{date_tag}_{cell_tag(lat, lon)}.gz"


def date_tag_from_input(input_path: str) -> str:
    """Extract date tag from CONUS filename, e.g. 'CONUS_010126.gz' -> '20260101'."""
    name = Path(input_path).stem  # CONUS_010126
    if name.startswith("CONUS_"):
        raw = name[6:]  # MMDDYY
        if len(raw) == 6:
            mm, dd, yy = raw[:2], raw[2:4], raw[4:6]
            return f"20{yy}{mm}{dd}"
    # Fallback: use filename as-is (strip extension)
    return Path(input_path).stem.replace(".", "_")


def shard(input_gz: str, lat_min: int, lat_max: int, lon_min: int, lon_max: int,
          skip_existing: bool = False) -> dict:
    """Single streaming pass over input_gz, writing one file per grid cell.

    Returns dict of stats per cell: {(lat,lon): {'records': N, 'size_bytes': M}}
    """
    date_tag = date_tag_from_input(input_gz)
    (GRID_DIR / date_tag).mkdir(parents=True, exist_ok=True)

    cells = [(lat, lon)
             for lat in range(lat_min, lat_max)
             for lon in range(lon_min, lon_max)]

    if not cells:
        print("No cells in range.", file=sys.stderr)
        return {}

    print(f"Sharding {input_gz} → {len(cells)} cell(s), date={date_tag}")
    print(f"  Region: lat [{lat_min},{lat_max}) × lon [{lon_min},{lon_max})")
    print(f"  Output: {GRID_DIR}/")

    # Open output file handles
    handles = {}
    for lat, lon in cells:
        out_path = output_path(date_tag, lat, lon)
        if skip_existing and out_path.exists():
            continue
        handles[(lat, lon)] = gzip.open(str(out_path), "wt")

    if not handles:
        print("All cells already exist, nothing to do.")
        return {}

    stats = {cell: {"records": 0} for cell in handles}

    t0 = time.time()
    records_in = 0
    records_out = 0

    with gzip.open(input_gz, "rt") as fin:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            records_in += 1

            try:
                rec = _json_loads(line)
            except Exception:
                continue

            lat = rec.get("lat")
            lon = rec.get("lon")
            if lat is None or lon is None:
                continue

            # Altitude ceiling
            alt = rec.get("alt_baro", 0)
            if isinstance(alt, (int, float)) and alt > ALT_CEILING_FT:
                continue

            # math.floor handles negative lons correctly (e.g. -122.3 → -123)
            cell_lat = math.floor(lat)
            cell_lon = math.floor(lon)

            key = (cell_lat, cell_lon)
            if key not in handles:
                continue

            handles[key].write(line + "\n")
            stats[key]["records"] += 1
            records_out += 1

            if records_in % 2_000_000 == 0:
                elapsed = time.time() - t0
                print(f"  {records_in/1e6:.0f}M scanned, {records_out:,} kept "
                      f"({elapsed:.0f}s)", flush=True)

    # Close all handles and record file sizes
    for key, fh in handles.items():
        fh.close()
        path = output_path(date_tag, key[0], key[1])
        stats[key]["size_bytes"] = path.stat().st_size

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s. Scanned {records_in:,} records, "
          f"kept {records_out:,} across {len(handles)} cells.")

    if len(cells) <= 50:
        for (lat, lon), s in sorted(stats.items()):
            size_kb = s.get("size_bytes", 0) / 1024
            print(f"  {cell_tag(lat,lon)}: {s['records']:,} records, {size_kb:.0f} KB")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Stage 2: Shard CONUS JSONL into 1°×1° grid cells.")
    parser.add_argument("--input", required=True,
                        help="Input CONUS JSONL gz file (e.g. data/CONUS_010126.gz)")
    parser.add_argument("--cell", nargs=2, type=float, metavar=("LAT", "LON"),
                        help="Single cell SW corner (e.g. --cell 37 -122)")
    parser.add_argument("--lat-min", type=int, help="Min latitude (inclusive)")
    parser.add_argument("--lat-max", type=int, help="Max latitude (exclusive)")
    parser.add_argument("--lon-min", type=int, help="Min longitude (inclusive)")
    parser.add_argument("--lon-max", type=int, help="Max longitude (exclusive)")
    parser.add_argument("--conus", action="store_true",
                        help="Shard all of CONUS (lat 24-50, lon -125 to -65)")
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip cells whose output file already exists")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"ERROR: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    if args.cell:
        lat_min = int(args.cell[0])
        lat_min = math.floor(args.cell[0])
        lon_min = math.floor(args.cell[1])
        lat_max = lat_min + 1
        lon_max = lon_min + 1
    elif args.conus:
        lat_min, lat_max = CONUS_LAT_MIN, CONUS_LAT_MAX
        lon_min, lon_max = CONUS_LON_MIN, CONUS_LON_MAX
    elif all(v is not None for v in [args.lat_min, args.lat_max, args.lon_min, args.lon_max]):
        lat_min, lat_max = args.lat_min, args.lat_max
        lon_min, lon_max = args.lon_min, args.lon_max
    else:
        parser.error("Specify --cell LAT LON, --conus, or --lat-min/max --lon-min/max")

    shard(args.input, lat_min, lat_max, lon_min, lon_max,
          skip_existing=args.skip_existing)


if __name__ == "__main__":
    main()
