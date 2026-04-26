#!/usr/bin/env python3
"""
Stage 5b: Extract traffic trips from grid cell shards for animated visualization.

Reads one or more grid cell shard GZs, reconstructs aircraft tracks, thins them,
normalizes timestamps to seconds-since-midnight, and writes a trips.json for use
with the deck.gl TripsLayer prototype.

Usage:
    python src/hotspots/stage5b_trips.py \
        --cell 37 -122 \
        --dates 20250501 20250502 20250503 \
        --output /tmp/trips.json
"""

import argparse
import gzip
import json
import math
import collections
from pathlib import Path

V2_GRID = Path("data/v2/grid")

# Thin to one point per this many seconds per aircraft
THIN_SECONDS = 10

# Drop tracks shorter than this
MIN_TRACK_PTS = 5

# Altitude filter: skip records clearly on the ground or in Class A
ALT_MIN_FT = 100
ALT_MAX_FT = 18000

# Max gap between consecutive points before starting a new track segment
MAX_GAP_SECONDS = 120


def stream_shard(path):
    with gzip.open(path, 'rb') as f:
        for line in f:
            try:
                r = json.loads(line)
            except Exception:
                continue
            if r.get('hex') and r.get('lat') and r.get('lon') and r.get('now'):
                yield r


def seconds_since_midnight(ts):
    """Convert unix timestamp to seconds since midnight UTC."""
    return ts % 86400


def extract_trips(shard_paths, thin_s=THIN_SECONDS):
    # hex -> list of (ts, lat, lon, alt) sorted by ts, one per thin bucket
    tracks = collections.defaultdict(dict)  # hex -> {bucket: (ts, lat, lon)}

    for path in shard_paths:
        print(f"  Reading {Path(path).name}...")
        for r in stream_shard(path):
            h = r['hex']
            ts = r['now']
            lat, lon = r['lat'], r['lon']
            alt = r.get('alt_baro')
            try:
                alt_ft = int(alt) if alt and alt != 'ground' else None
            except (ValueError, TypeError):
                alt_ft = None
            if alt_ft is None or not (ALT_MIN_FT <= alt_ft <= ALT_MAX_FT):
                continue
            bucket = int(ts // thin_s)
            if bucket not in tracks[h]:
                tracks[h][bucket] = (ts, lat, lon)

    trips = []
    for h, buckets in tracks.items():
        pts = sorted(buckets.values())  # sort by ts
        if len(pts) < MIN_TRACK_PTS:
            continue

        # Split on gaps > MAX_GAP_SECONDS into separate segments
        segments = []
        seg = [pts[0]]
        for p in pts[1:]:
            if p[0] - seg[-1][0] > MAX_GAP_SECONDS:
                segments.append(seg)
                seg = [p]
            else:
                seg.append(p)
        segments.append(seg)

        for seg in segments:
            if len(seg) < MIN_TRACK_PTS:
                continue
            trips.append({
                "path": [[p[2], p[1]] for p in seg],
                # Normalize to time-of-day so multi-day data overlaps
                "timestamps": [seconds_since_midnight(p[0]) for p in seg],
            })

    return trips


def main():
    parser = argparse.ArgumentParser(description="Extract trips JSON from grid cell shards")
    parser.add_argument("--cell", nargs=2, type=int, metavar=("LAT", "LON"), required=True)
    parser.add_argument("--dates", nargs="+", required=True,
                        help="Date tags e.g. 20250501 20250502")
    parser.add_argument("--output", default="/tmp/trips.json")
    parser.add_argument("--thin", type=int, default=THIN_SECONDS,
                        help=f"Thin to 1 point per N seconds (default {THIN_SECONDS})")
    args = parser.parse_args()

    lat, lon = args.cell
    cell_tag = f"{lat}_{lon}"

    shard_paths = []
    for date in args.dates:
        p = V2_GRID / date / f"{date}_{cell_tag}.gz"
        if p.exists():
            shard_paths.append(p)
        else:
            print(f"  WARNING: shard not found: {p}")

    if not shard_paths:
        print("No shards found.")
        return

    print(f"Extracting trips from {len(shard_paths)} shard(s), cell {cell_tag}...")
    trips = extract_trips(shard_paths, args.thin)
    print(f"  {len(trips)} track segments")

    sizes = [len(t['path']) for t in trips]
    if sizes:
        print(f"  Points per track: min={min(sizes)} max={max(sizes)} mean={sum(sizes)//len(sizes)}")

    out = {"trips": trips, "duration": 86400}
    with open(args.output, "w") as f:
        json.dump(out, f)
    size_kb = Path(args.output).stat().st_size // 1024
    print(f"  Written to {args.output} ({size_kb} KB)")


if __name__ == "__main__":
    main()
