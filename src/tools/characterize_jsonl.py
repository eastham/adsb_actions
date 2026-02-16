#!/usr/bin/env python3
"""Characterize a gzipped JSONL ADS-B shard file.

Reports geographic extent, altitude range, time span, unique aircraft,
data quality metrics, and per-aircraft statistics.

Usage:
    python src/tools/characterize_jsonl.py data/E16/062325_E16.gz
    python src/tools/characterize_jsonl.py data/E16/062325_E16.gz --field-elev 395
"""

import argparse
import gzip
import json
import math
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

# Add src/tools to path so we can import siblings
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_quality import analyze_shard_quality

# ADS-B emitter category descriptions (DO-260B 2.2.3.2.5.2)
CATEGORY_DESC = {
    "A0": "No category info",
    "A1": "Light (< 15,500 lbs)",
    "A2": "Small (15,500-75,000 lbs)",
    "A3": "Large (75,000-300,000 lbs)",
    "A4": "High vortex large (e.g. B757)",
    "A5": "Heavy (> 300,000 lbs)",
    "A6": "High performance (> 5G, 400+ kt)",
    "A7": "Rotorcraft",
    "B0": "No category info",
    "B1": "Glider/sailplane",
    "B2": "Lighter-than-air",
    "B3": "Parachutist/skydiver",
    "B4": "Ultralight/hang-glider/paraglider",
    "B5": "Reserved",
    "B6": "UAV",
    "B7": "Space/trans-atmospheric",
    "C0": "No category info",
    "C1": "Emergency surface vehicle",
    "C2": "Service surface vehicle",
    "C3": "Point obstacle (tethered balloon)",
    "C4": "Cluster obstacle",
    "C5": "Line obstacle",
}


def read_all_records(shard_gz: Path):
    """Yield all parseable records from a gzipped JSONL file (no altitude filter)."""
    with gzip.open(shard_gz, "rt") as f:
        for line in f:
            try:
                yield json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue


def characterize(shard_gz: Path, field_elev: int = 0):
    records = list(read_all_records(shard_gz))
    if not records:
        print("No records found.")
        return

    # --- Collect basic fields ---
    lats, lons, alts, timestamps = [], [], [], []
    by_hex: dict[str, list[dict]] = defaultdict(list)
    flights_seen: dict[str, set[str]] = defaultdict(set)  # hex -> set of callsigns
    categories = defaultdict(int)
    ground_count = 0
    no_position_count = 0
    no_alt_count = 0

    for r in records:
        ts = r.get("now")
        hex_id = r.get("hex")
        lat = r.get("lat")
        lon = r.get("lon")
        alt = r.get("alt_baro")
        flight = (r.get("flight") or "").strip()
        cat = r.get("category")

        if ts is not None:
            timestamps.append(ts)

        if hex_id:
            by_hex[hex_id].append(r)
            if flight:
                flights_seen[hex_id].add(flight)

        if lat is not None and lon is not None:
            lats.append(lat)
            lons.append(lon)
        else:
            no_position_count += 1

        if alt == "ground":
            ground_count += 1
        elif alt is not None:
            try:
                alts.append(int(alt))
            except (ValueError, TypeError):
                pass
        else:
            no_alt_count += 1

        if cat:
            categories[cat] += 1

    # --- Derived values ---
    total_points = len(records)
    unique_aircraft = len(by_hex)
    unique_callsigns = sum(len(cs) for cs in flights_seen.values())

    # Points per aircraft
    pts_per_ac = [len(pts) for pts in by_hex.values()]

    # Track durations per aircraft
    durations = []
    for hex_id, pts in by_hex.items():
        times = sorted(r.get("now", 0) for r in pts if r.get("now") is not None)
        if len(times) >= 2:
            durations.append(times[-1] - times[0])

    # --- Print report ---
    print(f"=== File: {shard_gz.name} ===\n")

    # Time info
    print("--- Time ---")
    if timestamps:
        t_min, t_max = min(timestamps), max(timestamps)
        dt_min = datetime.fromtimestamp(t_min, tz=timezone.utc)
        dt_max = datetime.fromtimestamp(t_max, tz=timezone.utc)
        span_h = (t_max - t_min) / 3600
        print(f"  First point:  {dt_min.strftime('%Y-%m-%d %H:%M:%S UTC')} (epoch {t_min})")
        print(f"  Last point:   {dt_max.strftime('%Y-%m-%d %H:%M:%S UTC')} (epoch {t_max})")
        print(f"  Time span:    {span_h:.1f} hours")
    print()

    # Volume
    print("--- Volume ---")
    print(f"  Total points:       {total_points:,}")
    print(f"  Unique aircraft:    {unique_aircraft:,}")
    print(f"  Unique callsigns:   {unique_callsigns:,}")
    print(f"  Points/aircraft:    "
          f"min={min(pts_per_ac)}, median={statistics.median(pts_per_ac):.0f}, "
          f"max={max(pts_per_ac)}, mean={statistics.mean(pts_per_ac):.1f}")
    if durations:
        print(f"  Track duration (s): "
              f"min={min(durations):.0f}, median={statistics.median(durations):.0f}, "
              f"max={max(durations):.0f}, mean={statistics.mean(durations):.0f}")
    print()

    # Geography
    print("--- Geography ---")
    if lats:
        cent_lat = statistics.mean(lats)
        cent_lon = statistics.mean(lons)
        print(f"  Centroid:     {cent_lat:.4f}, {cent_lon:.4f}")
        print(f"  Lat range:    {min(lats):.4f} to {max(lats):.4f}  "
              f"(span {max(lats)-min(lats):.4f} deg)")
        print(f"  Lon range:    {min(lons):.4f} to {max(lons):.4f}  "
              f"(span {max(lons)-min(lons):.4f} deg)")
        # Approximate bounding box size in nm
        lat_span_nm = (max(lats) - min(lats)) * 60
        avg_lat = (max(lats) + min(lats)) / 2
        lon_span_nm = (max(lons) - min(lons)) * 60 * math.cos(math.radians(avg_lat))
        print(f"  Bounding box: ~{lat_span_nm:.1f} x {lon_span_nm:.1f} nm")
        print(f"  No-position points: {no_position_count:,} "
              f"({100*no_position_count/total_points:.1f}%)")
    else:
        print("  No position data found")
    print()

    # Altitude
    print("--- Altitude ---")
    if alts:
        print(f"  Range:        {min(alts):,} to {max(alts):,} ft")
        print(f"  Median:       {statistics.median(alts):,.0f} ft")
        print(f"  Mean:         {statistics.mean(alts):,.0f} ft")
        if field_elev:
            agl_alts = [a - field_elev for a in alts]
            print(f"  AGL range:    {min(agl_alts):,} to {max(agl_alts):,} ft "
                  f"(field elev {field_elev} ft)")
    else:
        print("  No altitude data")
    print(f"  Ground reports:     {ground_count:,}")
    print(f"  No-altitude points: {no_alt_count:,}")
    print()

    # Aircraft categories
    if categories:
        print("--- Aircraft Categories ---")
        for cat, cnt in sorted(categories.items(), key=lambda x: -x[1]):
            desc = CATEGORY_DESC.get(cat, "Unknown")
            print(f"  {cat} {desc}: {cnt:,} ({100*cnt/total_points:.1f}%)")
        print()

    # Data quality (from analyze_shard_quality)
    print(f"--- Data Quality (analyze_shard_quality) NOTE using field_elev {field_elev} ---")
    if lats:
        cent_lat = statistics.mean(lats)
        cent_lon = statistics.mean(lons)
        quality = analyze_shard_quality(
            shard_gz, field_elev=field_elev,
            airport_lat=cent_lat, airport_lon=cent_lon
        )
        if quality:
            lost_rate = quality["lost_rate"]
            print(f"  Total tracks:       {quality['total_tracks']:,}")
            print(f"  Low-alt tracks:     {quality['low_alt_tracks']:,}")
            print(f"  Completed tracks:   {quality['completed_tracks']:,}")
            print(f"  Lost tracks:        {quality['lost_tracks']:,}")
            print(f"  Lost rate:          "
                  + (f"{lost_rate:.1%}" if lost_rate is not None else "N/A"))
            print(f"  Median gap:         "
                  + (f"{quality['median_gap_s']:.1f}s" if quality['median_gap_s'] is not None else "N/A"))
            print(f"  P90 gap:            "
                  + (f"{quality['p90_gap_s']:.1f}s" if quality['p90_gap_s'] is not None else "N/A"))
            print(f"  Total gaps measured: {quality['total_gaps']:,}")
        else:
            print("  No quality data (shard may be empty after filtering)")
    else:
        print("  Skipped (no position data for centroid)")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Characterize a gzipped JSONL ADS-B shard file"
    )
    parser.add_argument("shard", type=Path, help="Path to .gz JSONL shard")
    parser.add_argument("--field-elev", type=int, default=0,
                        help="Airport field elevation in feet (for AGL and quality metrics)")
    args = parser.parse_args()

    if not args.shard.exists():
        print(f"Error: {args.shard} not found", file=sys.stderr)
        sys.exit(1)

    characterize(args.shard, field_elev=args.field_elev)


if __name__ == "__main__":
    main()
