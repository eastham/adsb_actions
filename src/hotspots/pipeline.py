#!/usr/bin/env python3
"""
v2 LOS Pipeline Orchestrator

Runs stages 2–5 over a date range and geographic region:
  Stage 2: Shard CONUS JSONL into 1°×1° grid cells
            source: data/  (CONUS_DDMMYY.gz files, override with --conus-dir)
            dest:   data/v2/grid/<date_tag>/<date_tag>_<lat>_<lon>.gz
  Stage 3: Run LOS analysis on each cell shard (parallelizable)
            source: data/v2/grid/
            dest:   data/v2/events/<date_tag>/<date_tag>_<lat>_<lon>.{csv,parquet}
  Stage 4: Aggregate per-cell Parquets into a regional Parquet
            source: data/v2/events/
            dest:   data/v2/regional/<region>_<start>_<end>.parquet
  Stage 5: Generate map HTML (self-contained or PMTiles)
            source: data/v2/regional/
            dest:   data/v2/maps/<region>_<start>_<end>.html

Usage:
    # Bay Area → Nevada band, 21 cells, 8 workers, PMTiles output:
    python src/hotspots/pipeline.py \\
        --start-date 20260101 --end-date 20260131 \\
        --lat-min 36 --lat-max 39 --lon-min -122 --lon-max -115 \\
        --workers 8 --pmtiles

    # Named region (CA):
    python src/hotspots/pipeline.py \\
        --start-date 20260101 --end-date 20260131 \\
        --region CA --workers 8 --pmtiles

    # Skip sharding (shards already exist), just re-analyze + visualize:
    python src/hotspots/pipeline.py \\
        --start-date 20260101 --end-date 20260131 \\
        --lat-min 36 --lat-max 39 --lon-min -122 --lon-max -115 \\
        --skip-stage2 --workers 8

    # Just re-visualize an existing regional Parquet:
    python src/hotspots/stage5_visualize.py \\
        --input data/v2/regional/36_39_-122_-115_20260101_20260131.parquet \\
        --pmtiles
"""

import argparse
import datetime
import glob as glob_module
import math
import os
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
for _p in [str(_ROOT / "src"), str(_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from hotspots.stage2_shard import (
    shard, date_tag_from_input,
    CONUS_LAT_MIN, CONUS_LAT_MAX, CONUS_LON_MIN, CONUS_LON_MAX,
    GRID_DIR,
)
from hotspots.stage3_analyze import find_shards, analyze_shards, EVENTS_DIR
from hotspots.stage4_aggregate import aggregate, find_parquet_files, REGIONS, REGIONAL_DIR
from hotspots.stage5_visualize import (
    generate_html,
    generate_pmtiles,
    generate_pmtiles_html,
    write_event_sidecars,
    load_events,
    MAPS_DIR,
    build_us_airports_lookup,
    _parse_date_range_from_stem,
)

import pandas as pd


# Named-region bounds (must match stage4_aggregate REGIONS keys)
# Provided here so the CLI can use them without importing stage4 at parse time.
_NAMED_REGIONS = {
    name: (bb["lat_min"], bb["lat_max"], bb["lon_min"], bb["lon_max"])
    for name, bb in REGIONS.items()
}


# Cells to skip for known high-density events that flood the prox detector with false positives.
# Each entry: (lat, lon, start_date_YYYYMMDD, end_date_YYYYMMDD, reason)
CELL_EXCLUSIONS = [
    (43, -89, "20250719", "20250727", "EAA AirVenture Oshkosh"),
]


def _is_excluded(path: Path, date_tag: str) -> bool:
    parts = path.stem.split("_")
    if len(parts) < 3:
        return False
    try:
        lat, lon = int(parts[1]), int(parts[2])
    except ValueError:
        return False
    for ex_lat, ex_lon, start, end, reason in CELL_EXCLUSIONS:
        if lat == ex_lat and lon == ex_lon and start <= date_tag <= end:
            print(f"  [excluded] {path.stem} — {reason}")
            return True
    return False


def _cell_in_box(path: Path, lat_min, lat_max, lon_min, lon_max) -> bool:
    parts = path.stem.split("_")
    if len(parts) < 3:
        return False
    try:
        lat, lon = int(parts[1]), int(parts[2])
        return lat_min <= lat < lat_max and lon_min <= lon < lon_max
    except ValueError:
        return False


def _conus_path(date: datetime.date, conus_dir: str) -> Path:
    """Return path to CONUS gz file for a date, e.g. data/CONUS_010126.gz."""
    tag = date.strftime("%m%d%y")
    return Path(conus_dir) / f"CONUS_{tag}.gz"


def _date_range(start: datetime.date, end: datetime.date):
    d = start
    while d <= end:
        yield d
        d += datetime.timedelta(days=1)


def run_stages_23(
    date: datetime.date,
    lat_min: int, lat_max: int, lon_min: int, lon_max: int,
    conus_dir: str,
    workers: int = 1,
    animate: bool = False,
    skip_shard: bool = False,
    skip_existing: bool = False,
) -> dict:
    """
    Run Stage 2 (shard) + Stage 3 (analyze) for one date.

    Returns stats dict: {date_tag, shard_s, shard_kb, analyze_s, events, errors}
    """
    conus_gz = _conus_path(date, conus_dir)
    date_tag = date.strftime("%Y%m%d")
    stats = {"date_tag": date_tag, "shard_s": 0, "shard_kb": 0,
             "analyze_s": 0, "events": 0, "errors": 0}

    # Stage 2
    if skip_shard:
        for lat in range(lat_min, lat_max):
            for lon in range(lon_min, lon_max):
                p = GRID_DIR / date_tag / f"{date_tag}_{lat}_{lon}.gz"
                if p.exists():
                    stats["shard_kb"] += p.stat().st_size // 1024
    else:
        if not conus_gz.exists():
            print(f"  [WARN] CONUS file not found: {conus_gz} — skipping {date_tag}")
            stats["errors"] += 1
            return stats
        t0 = time.time()
        shard(str(conus_gz), lat_min, lat_max, lon_min, lon_max,
              skip_existing=skip_existing)
        stats["shard_s"] = time.time() - t0
        for lat in range(lat_min, lat_max):
            for lon in range(lon_min, lon_max):
                p = GRID_DIR / date_tag / f"{date_tag}_{lat}_{lon}.gz"
                if p.exists():
                    stats["shard_kb"] += p.stat().st_size // 1024

    # Stage 3
    day_shards = [s for s in find_shards(GRID_DIR, date_tag)
                  if _cell_in_box(s, lat_min, lat_max, lon_min, lon_max)
                  and not _is_excluded(s, date_tag)]
    if day_shards:
        t0 = time.time()
        results = analyze_shards(day_shards, workers=workers, animate=animate,
                                 skip_existing=skip_existing)
        stats["analyze_s"] = time.time() - t0
        stats["events"] = sum(v["events"] or 0 for v in results.values()
                               if v["error"] is None)
        stats["errors"] += sum(1 for v in results.values()
                                if v["error"] and v["error"] != "skipped")

    return stats


def run_stage4(
    date_tags: list,
    lat_min: int, lat_max: int, lon_min: int, lon_max: int,
    region_label: str,
    output_path: str,
) -> pd.DataFrame:
    """Aggregate per-cell Parquets for all date_tags into a regional Parquet."""
    parquet_files = find_parquet_files(EVENTS_DIR, date_tags,
                                       lat_min, lat_max, lon_min, lon_max)
    df = aggregate(parquet_files)
    REGIONAL_DIR.mkdir(parents=True, exist_ok=True)
    if not df.empty:
        df.to_parquet(output_path, index=False)
        print(f"  Aggregated {len(df):,} events → {output_path}")
    else:
        print(f"  No events found for region {region_label}.")
    return df


def run_stage5(
    df: pd.DataFrame,
    output_path: str,
    pmtiles: bool,
    zoom: float | None,
    traffic_tile_dir: str = "https://airbornehotspots.org/tiles",
    asset_stem: str | None = None,
) -> None:
    """Generate map HTML (self-contained or PMTiles) from a DataFrame of events.
    `zoom=None` means auto-fit to data bounds on load (whole region visible);
    pass an explicit value to override.
    """
    MAPS_DIR.mkdir(parents=True, exist_ok=True)
    if df.empty:
        print("  Stage 5 skipped: no events.")
        return

    center_lat = float(df["lat"].mean())
    center_lon = float(df["lon"].mean())

    auto_fit = zoom is None
    static_zoom = zoom if zoom is not None else 7.0  # fallback if AUTO_FIT JS fails
    bounds = (
        float(df["lon"].min()), float(df["lat"].min()),
        float(df["lon"].max()), float(df["lat"].max()),
    )

    # Date range parsed from the output stem (e.g. *_20250601_20250831.html);
    # airports lookup baked into the HTML for the upper-left jump box.
    stem = Path(output_path).stem
    date_range = _parse_date_range_from_stem(stem)
    airports_lookup = build_us_airports_lookup()

    if pmtiles:
        pmtiles_path = str(Path(output_path).with_suffix(".pmtiles"))
        sidecar_dir = output_path.replace(".html", "_tracks")
        generate_pmtiles(df, pmtiles_path)
        print(f"  Writing {len(df):,} event track sidecars...")
        write_event_sidecars(df, sidecar_dir)
        alt_bands = sorted(df["alt_band"].dropna().unique().tolist()) if "alt_band" in df.columns else []
        html = generate_pmtiles_html(pmtiles_path, sidecar_dir,
                                     center_lat, center_lon, static_zoom, alt_bands,
                                     traffic_tile_dir=traffic_tile_dir,
                                     date_range=date_range,
                                     airports_lookup=airports_lookup,
                                     asset_stem=asset_stem,
                                     bounds=bounds,
                                     auto_fit=auto_fit)
    else:
        html = generate_html(df, center_lat, center_lon, static_zoom,
                             traffic_tile_dir=traffic_tile_dir,
                             date_range=date_range,
                             airports_lookup=airports_lookup,
                             bounds=bounds,
                             auto_fit=auto_fit)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    size_mb = os.path.getsize(output_path) / 1024 / 1024
    print(f"  Map: {output_path}  ({size_mb:.1f} MB)")
    if pmtiles:
        print(f"  Serve: python src/hotspots/serve.py . 8080")
        print(f"  Open:  http://localhost:8080/{output_path}")
    else:
        print(f"  Open:  file://{os.path.abspath(output_path)}")


# ---------------------------------------------------------------------------
# Summary / benchmarking
# ---------------------------------------------------------------------------

def _print_summary(all_stats: list, lat_min, lat_max, lon_min, lon_max,
                   wall_elapsed: float):
    n_cells = (lat_max - lat_min) * (lon_max - lon_min)
    n_days = len(all_stats)
    total_shard_s = sum(s["shard_s"] for s in all_stats)
    total_analyze_s = sum(s["analyze_s"] for s in all_stats)
    total_events = sum(s["events"] for s in all_stats)
    total_errors = sum(s["errors"] for s in all_stats)
    total_shard_kb = sum(s["shard_kb"] for s in all_stats)
    skipped_shard = sum(1 for s in all_stats if s["shard_s"] == 0 and not s["errors"])

    avg_shard_kb = total_shard_kb / n_days if n_days else 0
    avg_analyze_s = total_analyze_s / n_days if n_days else 0
    avg_events = total_events / n_days if n_days else 0

    print(f"\n{'─'*75}")
    print(f"{'Date':<12} {'Shard KB':>10} {'Shard s':>8} {'Analyze s':>10} {'Events':>8}")
    print(f"{'─'*75}")
    for s in all_stats:
        shard_note = "(skip)" if s["shard_s"] == 0 else f"{s['shard_s']:.0f}"
        print(f"{s['date_tag']:<12} {s['shard_kb']:>10,} {shard_note:>8} "
              f"{s['analyze_s']:>10.0f} {s['events']:>8}")
    print(f"{'─'*75}")
    print(f"{'TOTAL':<12} {total_shard_kb:>10,} {total_shard_s:>8.0f} "
          f"{total_analyze_s:>10.0f} {total_events:>8}")
    print(f"{'AVG/DAY':<12} {avg_shard_kb:>10,.0f} {'':>8} "
          f"{avg_analyze_s:>10.1f} {avg_events:>8.1f}")
    print(f"{'─'*75}")
    print(f"\nTotal wall time: {wall_elapsed/60:.1f} min | Errors: {total_errors}")

    # Extrapolation
    # Rectangular bounding box cell count — actual land cells are ~60% of this
    # due to ocean/Canada, but this is the correct upper bound for the full CONUS pass.
    CONUS_CELLS = (CONUS_LAT_MAX - CONUS_LAT_MIN) * (CONUS_LON_MAX - CONUS_LON_MIN)
    GLOBAL_CELLS = 3000

    if skipped_shard < n_days:
        measured_shard_s = total_shard_s / (n_days - skipped_shard)
    else:
        measured_shard_s = 90  # prototype fallback

    analyze_s_per_cell_day = avg_analyze_s / n_cells if n_cells > 0 else avg_analyze_s
    shard_kb_per_cell_day = avg_shard_kb / n_cells if n_cells > 0 else avg_shard_kb

    print(f"\nEXTRAPOLATION (based on {n_cells} cells × {n_days} days)")
    print(f"  Shard time/CONUS-file:   {measured_shard_s:.0f}s")
    print(f"  Analyze time/cell/day:   {analyze_s_per_cell_day:.1f}s")
    print(f"  Shard size/cell/day:     {shard_kb_per_cell_day:.0f} KB")
    print(f"  Events/cell/day:         {avg_events/n_cells:.1f}")

    for label, cells in [("CONUS", CONUS_CELLS), ("Global", GLOBAL_CELLS)]:
        analyze_s_day = analyze_s_per_cell_day * cells
        shard_mb_day = shard_kb_per_cell_day * cells / 1024
        shard_gb_30 = shard_mb_day * 30 / 1024

        print(f"\n  [{label}: {cells} cells]")
        print(f"    Shard time/day:    {measured_shard_s/60:.1f} min (one CONUS pass)")
        print(f"    Analyze time/day:  {analyze_s_day/60:.1f} min serial  "
              f"→ {analyze_s_day/60/8:.1f} min on 8 cores  "
              f"→ {analyze_s_day/60/32:.1f} min on 32 cores")
        print(f"    Shard storage/30d: {shard_gb_30:.1f} GB")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="v2 LOS Pipeline: shard → analyze → aggregate → visualize")

    # Date range
    parser.add_argument("--start-date", required=True,
                        help="Start date YYYYMMDD")
    parser.add_argument("--end-date", required=True,
                        help="End date YYYYMMDD (inclusive)")

    # Region
    region_group = parser.add_mutually_exclusive_group(required=True)
    region_group.add_argument("--region", choices=list(_NAMED_REGIONS.keys()),
                               help="Named region")
    region_group.add_argument("--lat-min", type=int, help="Min latitude (inclusive)")
    parser.add_argument("--lat-max", type=int)
    parser.add_argument("--lon-min", type=int)
    parser.add_argument("--lon-max", type=int)

    # Stage 3 parallelism
    parser.add_argument("--workers", type=int, default=1,
                        help="Parallel workers for Stage 3 (default: 1)")
    parser.add_argument("--animate", action="store_true",
                        help="Generate animation HTMLs in Stage 3")

    # Stage 5
    parser.add_argument("--pmtiles", action="store_true",
                        help="Stage 5: PMTiles output (required for large datasets)")
    parser.add_argument("--zoom", type=float, default=None,
                        help="Initial map zoom. Default: auto-fit to data bounds "
                             "(whole region visible). Pass an explicit value to override.")
    parser.add_argument("--traffic-tiles", type=str,
                        default="https://airbornehotspots.org/tiles",
                        help="Traffic tile URL or local path prefix (default: production URL; "
                             "use ../../../tiles/traffic for local dev)")
    parser.add_argument("--asset-stem", type=str, default=None,
                        help="Override the inlined .pmtiles / _tracks filenames in the "
                             "generated HTML (e.g. --asset-stem conus). Used when the "
                             "deployer publishes a stable-named alias separate from the "
                             "dated source files. Only takes effect with --pmtiles.")

    # Skip flags
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip cells/dates whose outputs already exist")
    parser.add_argument("--skip-stage2", action="store_true",
                        help="Skip Stage 2 (shards already exist)")
    parser.add_argument("--skip-stage3", action="store_true",
                        help="Skip Stage 3 (per-cell analysis already done)")
    parser.add_argument("--skip-stage4", action="store_true",
                        help="Skip Stage 4 (aggregation)")
    parser.add_argument("--skip-stage5", action="store_true",
                        help="Skip Stage 5 (visualization)")

    # Paths
    parser.add_argument("--conus-dir", default="data",
                        help="Directory containing CONUS_*.gz files (default: data)")

    args = parser.parse_args()

    # Resolve region
    if args.region:
        lat_min, lat_max, lon_min, lon_max = _NAMED_REGIONS[args.region]
        region_label = args.region
    else:
        if None in (args.lat_max, args.lon_min, args.lon_max):
            parser.error("--lat-min requires --lat-max, --lon-min, --lon-max")
        lat_min, lat_max = args.lat_min, args.lat_max
        lon_min, lon_max = args.lon_min, args.lon_max
        region_label = f"{lat_min}_{lat_max}_{lon_min}_{lon_max}"

    # Parse dates
    try:
        start_date = datetime.datetime.strptime(args.start_date, "%Y%m%d").date()
        end_date   = datetime.datetime.strptime(args.end_date,   "%Y%m%d").date()
    except ValueError as e:
        parser.error(f"Invalid date: {e}")

    if end_date < start_date:
        parser.error("--end-date must be >= --start-date")

    n_cells = (lat_max - lat_min) * (lon_max - lon_min)
    n_days  = (end_date - start_date).days + 1
    regional_parquet = str(REGIONAL_DIR / f"{region_label}_{args.start_date}_{args.end_date}.parquet")
    output_html      = str(MAPS_DIR     / f"{region_label}_{args.start_date}_{args.end_date}.html")

    print(f"v2 LOS Pipeline")
    print(f"  Dates:   {args.start_date} – {args.end_date}  ({n_days} day(s))")
    print(f"  Region:  {region_label}  lat [{lat_min},{lat_max}) × lon [{lon_min},{lon_max})  ({n_cells} cells)")
    print(f"  Workers: {args.workers}  |  PMTiles: {args.pmtiles}")

    wall_start = time.time()
    all_stats = []

    # Stages 2 + 3 — one date at a time
    if not (args.skip_stage2 and args.skip_stage3):
        dates = list(_date_range(start_date, end_date))
        for i, d in enumerate(dates, 1):
            date_str = d.strftime("%Y%m%d")
            stage2_label = "Stage 2: shard CONUS→grid" if not args.skip_stage2 else "Stage 2: skipped"
            print(f"\n[day {i}/{n_days}] {date_str}  |  {stage2_label}  →  Stage 3: LOS analysis")
            stats = run_stages_23(
                date=d,
                lat_min=lat_min, lat_max=lat_max,
                lon_min=lon_min, lon_max=lon_max,
                conus_dir=args.conus_dir,
                workers=args.workers,
                animate=args.animate,
                skip_shard=args.skip_stage2,
                skip_existing=args.skip_existing,
            )
            all_stats.append(stats)
            print(f"  shard: {stats['shard_kb']:,} KB  analyze: {stats['analyze_s']:.0f}s  "
                  f"events: {stats['events']}")

    # Stage 4: aggregate
    df = pd.DataFrame()
    if args.skip_stage4 and Path(regional_parquet).exists():
        df = load_events(regional_parquet)
        print(f"\nStage 4 skipped — loaded {len(df):,} events from {regional_parquet}")
    else:
        if args.skip_stage4:
            print(f"\nStage 4: regional Parquet not found, aggregating from per-cell files...")
        else:
            print(f"\nStage 4: aggregate per-cell Parquets → regional Parquet ({(end_date - start_date).days + 1} day(s))...")
        date_tags = [d.strftime("%Y%m%d") for d in _date_range(start_date, end_date)]
        t4 = time.time()
        df = run_stage4(date_tags, lat_min, lat_max, lon_min, lon_max,
                        region_label, regional_parquet)
        print(f"  Stage 4 done in {time.time()-t4:.0f}s")

    # Stage 5: visualize
    if not args.skip_stage5:
        mode = "PMTiles" if args.pmtiles else "self-contained HTML"
        print(f"\nStage 5: generate map ({mode})...")
        t5 = time.time()
        run_stage5(df, output_html, pmtiles=args.pmtiles, zoom=args.zoom,
                   traffic_tile_dir=args.traffic_tiles,
                   asset_stem=args.asset_stem)
        print(f"  Stage 5 done in {time.time()-t5:.0f}s")

    # Summary
    wall_elapsed = time.time() - wall_start
    if all_stats:
        _print_summary(all_stats, lat_min, lat_max, lon_min, lon_max, wall_elapsed)
    else:
        print(f"\nDone in {wall_elapsed/60:.1f} min")


if __name__ == "__main__":
    main()
