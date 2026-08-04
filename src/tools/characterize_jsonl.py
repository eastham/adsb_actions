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
from datetime import date, datetime, timezone
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


def read_all_records(shards):
    """Yield all parseable records from one or more gzipped JSONL files."""
    if isinstance(shards, Path):
        shards = [shards]
    for shard_gz in shards:
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


def characterize(shard_gz, field_elev: int = 0,
                 tail_histogram: bool = False, min_days: int = 2):
    """shard_gz may be a single Path or a list of Paths."""
    shards = [shard_gz] if isinstance(shard_gz, Path) else list(shard_gz)
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
    ac_days: dict[str, set] = defaultdict(set)  # hex_id -> set of ISO date strings
    ac_cat: dict[str, dict] = defaultdict(lambda: defaultdict(int))  # hex_id -> category counts
    categories = defaultdict(int)
    total_points = 0
    ground_count = 0
    no_position_count = 0
    no_alt_count = 0

    for r in read_all_records(shards):
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
                ac_days[hex_id].add(date.fromtimestamp(ts).isoformat())
            if flight:
                flights_seen[hex_id].add(flight)
            if cat:
                ac_cat[hex_id][cat] += 1

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
    if len(shards) == 1:
        print(f"=== File: {shards[0].name} ===\n")
    else:
        print(f"=== {len(shards)} files: {shards[0].name} .. {shards[-1].name} ===\n")

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

    # Tail number histogram (optional)
    if tail_histogram:
        # Sort by days seen descending, then by points as tiebreaker
        sorted_ac = sorted(ac_pts.keys(),
                           key=lambda h: (len(ac_days[h]), ac_pts[h]), reverse=True)
        above_threshold = [(h, len(ac_days[h])) for h in sorted_ac
                           if len(ac_days[h]) >= min_days]

        print(f"--- Tail Number Histogram (>= {min_days} days) ---")
        for hex_id, days in above_threshold:
            tails = sorted(flights_seen[hex_id])
            tail_str = tails[0] if tails else "(no callsign)"
            top_cat = max(ac_cat[hex_id], key=ac_cat[hex_id].get) if ac_cat[hex_id] else "?"
            cat_desc = CATEGORY_DESC.get(top_cat, top_cat)
            print(f"  {tail_str:<10} ({hex_id}): {days} days  [{top_cat} {cat_desc}]")
        print()

        print(f"--- YAML aircraft_lists block (>= {min_days} days) ---")
        print("aircraft_lists:")
        print("  participating:")
        for hex_id, days in above_threshold:
            tails = sorted(flights_seen[hex_id])
            tail_str = tails[0] if tails else None
            if tail_str:
                top_cat = max(ac_cat[hex_id], key=ac_cat[hex_id].get) if ac_cat[hex_id] else "?"
                print(f"    - {tail_str:<10}  # {days} days  {top_cat} {CATEGORY_DESC.get(top_cat, '')}")
        print()

    # Data quality (from analyze_shard_quality) — single file only
    print(f"--- Data Quality (analyze_shard_quality) NOTE using field_elev {field_elev} ---")
    if len(shards) > 1:
        print("  Skipped for multi-file analysis")
        print()
        return
    if lat_stats.count:
        cent_lat = lat_stats.mean
        cent_lon = lon_stats.mean
        quality = analyze_shard_quality(
            shards[0], field_elev=field_elev,
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
    parser.add_argument("shards", type=Path, nargs="+", help="Path(s) to .gz JSONL shard(s)")
    parser.add_argument("--field-elev", type=int, default=0,
                        help="Airport field elevation in feet (for AGL and quality metrics)")
    parser.add_argument("--tail-histogram", action="store_true",
                        help="Print tail number histogram and YAML aircraft_lists block")
    parser.add_argument("--min-days", type=int, default=2,
                        help="Minimum days seen to include in histogram/YAML (default: 2)")
    args = parser.parse_args()

    missing = [str(p) for p in args.shards if not p.exists()]
    if missing:
        print(f"Error: not found: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    characterize(args.shards, field_elev=args.field_elev,
                 tail_histogram=args.tail_histogram, min_days=args.min_days)


if __name__ == "__main__":
    main()
