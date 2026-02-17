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


class RunningStats:
    """Track min/max/sum/count for a numeric stream without storing values."""
    def __init__(self):
        self.min = float('inf')
        self.max = float('-inf')
        self.sum = 0.0
        self.count = 0
        self._values_for_median = []  # only used if median needed

    def add(self, val, keep_for_median=False):
        if val < self.min:
            self.min = val
        if val > self.max:
            self.max = val
        self.sum += val
        self.count += 1
        if keep_for_median:
            self._values_for_median.append(val)

    @property
    def mean(self):
        return self.sum / self.count if self.count else 0

    @property
    def median(self):
        if self._values_for_median:
            return statistics.median(self._values_for_median)
        return None


def characterize(shard_gz: Path, field_elev: int = 0):
    # Stream through records once, accumulating only stats (not raw records)
    lat_stats = RunningStats()
    lon_stats = RunningStats()
    alt_stats = RunningStats()
    ts_stats = RunningStats()

    # Per-aircraft: only track point count and time range
    ac_pts: dict[str, int] = defaultdict(int)
    ac_time_min: dict[str, float] = {}
    ac_time_max: dict[str, float] = {}
    flights_seen: dict[str, set[str]] = defaultdict(set)
    categories = defaultdict(int)
    total_points = 0
    ground_count = 0
    no_position_count = 0
    no_alt_count = 0

    for r in read_all_records(shard_gz):
        total_points += 1
        ts = r.get("now")
        hex_id = r.get("hex")
        lat = r.get("lat")
        lon = r.get("lon")
        alt = r.get("alt_baro")
        flight = (r.get("flight") or "").strip()
        cat = r.get("category")

        if ts is not None:
            ts_stats.add(ts)

        if hex_id:
            ac_pts[hex_id] += 1
            if ts is not None:
                if hex_id not in ac_time_min or ts < ac_time_min[hex_id]:
                    ac_time_min[hex_id] = ts
                if hex_id not in ac_time_max or ts > ac_time_max[hex_id]:
                    ac_time_max[hex_id] = ts
            if flight:
                flights_seen[hex_id].add(flight)

        if lat is not None and lon is not None:
            lat_stats.add(lat)
            lon_stats.add(lon)
        else:
            no_position_count += 1

        if alt == "ground":
            ground_count += 1
        elif alt is not None:
            try:
                alt_stats.add(int(alt), keep_for_median=True)
            except (ValueError, TypeError):
                pass
        else:
            no_alt_count += 1

        if cat:
            categories[cat] += 1

    if total_points == 0:
        print("No records found.")
        return

    # --- Derived values ---
    unique_aircraft = len(ac_pts)
    unique_callsigns = sum(len(cs) for cs in flights_seen.values())

    pts_per_ac = list(ac_pts.values())

    # Track durations from min/max timestamps per aircraft
    durations = []
    for hex_id in ac_pts:
        if hex_id in ac_time_min and hex_id in ac_time_max:
            dur = ac_time_max[hex_id] - ac_time_min[hex_id]
            if dur > 0:
                durations.append(dur)

    # --- Print report ---
    print(f"=== File: {shard_gz.name} ===\n")

    # Time info
    print("--- Time ---")
    if ts_stats.count:
        dt_min = datetime.fromtimestamp(ts_stats.min, tz=timezone.utc)
        dt_max = datetime.fromtimestamp(ts_stats.max, tz=timezone.utc)
        span_h = (ts_stats.max - ts_stats.min) / 3600
        print(f"  First point:  {dt_min.strftime('%Y-%m-%d %H:%M:%S UTC')} (epoch {ts_stats.min})")
        print(f"  Last point:   {dt_max.strftime('%Y-%m-%d %H:%M:%S UTC')} (epoch {ts_stats.max})")
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
    if lat_stats.count:
        cent_lat = lat_stats.mean
        cent_lon = lon_stats.mean
        print(f"  Centroid:     {cent_lat:.4f}, {cent_lon:.4f}")
        print(f"  Lat range:    {lat_stats.min:.4f} to {lat_stats.max:.4f}  "
              f"(span {lat_stats.max-lat_stats.min:.4f} deg)")
        print(f"  Lon range:    {lon_stats.min:.4f} to {lon_stats.max:.4f}  "
              f"(span {lon_stats.max-lon_stats.min:.4f} deg)")
        # Approximate bounding box size in nm
        lat_span_nm = (lat_stats.max - lat_stats.min) * 60
        avg_lat = (lat_stats.max + lat_stats.min) / 2
        lon_span_nm = (lon_stats.max - lon_stats.min) * 60 * math.cos(math.radians(avg_lat))
        print(f"  Bounding box: ~{lat_span_nm:.1f} x {lon_span_nm:.1f} nm")
        print(f"  No-position points: {no_position_count:,} "
              f"({100*no_position_count/total_points:.1f}%)")
    else:
        print("  No position data found")
    print()

    # Altitude
    print("--- Altitude ---")
    if alt_stats.count:
        print(f"  Range:        {alt_stats.min:,} to {alt_stats.max:,} ft")
        alt_median = alt_stats.median
        print(f"  Median:       {alt_median:,.0f} ft" if alt_median is not None else "  Median:       N/A")
        print(f"  Mean:         {alt_stats.mean:,.0f} ft")
        if field_elev:
            print(f"  AGL range:    {alt_stats.min - field_elev:,} to {alt_stats.max - field_elev:,} ft "
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
    if lat_stats.count:
        cent_lat = lat_stats.mean
        cent_lon = lon_stats.mean
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
