"""v2 airport-data-quality driver.

Computes per-airport ADS-B receiver-coverage scores across the v2 grid
shards in a single pass and writes an aggregate JSON keyed by ICAO. Reuses
the v1 quality algorithm (analyze_shard_quality + aggregate_per_date_results
in src/tools/data_quality.py) verbatim — this module only handles the
plumbing: airport-set selection, cell discovery, per-airport record
extraction with a shared cell cache.

Output format mirrors v1's per-airport {ICAO}_quality.json, except
score="none" entries are emitted for airports whose shards yielded no
usable data (so the v2 map can render a grey "?" rather than silently
degrading to yellow).

CLI:

    python -m src.tools.v2_airport_quality \\
        --grid-dir data/v2/grid \\
        --start-date 20260101 --end-date 20260131 \\
        --output data/v2/airport_quality.json \\
        [--airport KWVI]   # debugging: restrict to one airport
        [--verbose]        # log per-airport, per-date metrics
"""

from __future__ import annotations

import argparse
import csv
import datetime
import gzip
import json
import logging
import math
import sys
from collections import defaultdict
from pathlib import Path

try:
    import orjson as _json
except ImportError:
    import json as _json

# Ensure src/ is importable when run as a script.
_ROOT = Path(__file__).resolve().parents[2]
for _p in [str(_ROOT / "src"), str(_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from tools.data_quality import (
    LOW_ALT_RADIUS_NM,
    _fast_distance_nm,
    analyze_shard_quality,
    aggregate_per_date_results,
)
from tools.generate_airport_config import AIRPORTS_URL, download_with_cache

logger = logging.getLogger(__name__)

V2_GRID_DIR = Path("data/v2/grid")
V2_AQ_DIR = Path("data/v2/aq")           # per-day raw scores live here
DEFAULT_OUTPUT = Path("data/v2/airport_quality.json")

NM_PER_DEG_LAT = 60.0

# OurAirports types we score (matches stage5_visualize.build_us_airports_lookup).
KEEP_AIRPORT_TYPES = {"large_airport", "medium_airport", "small_airport"}


# ---------------------------------------------------------------------------
# Airport lookup with elevation
# ---------------------------------------------------------------------------

def load_us_airports_with_elev() -> dict[str, dict]:
    """Return {IDENT: {lat, lon, elev_ft}} for US public airports.

    Mirrors stage5_visualize.build_us_airports_lookup but also includes
    field elevation, which the quality algorithm needs for AGL bands.
    Cached on disk via download_with_cache.
    """
    airports_path = download_with_cache(AIRPORTS_URL, "airports.csv")
    out: dict[str, dict] = {}
    with open(airports_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("iso_country") != "US":
                continue
            if row.get("type") not in KEEP_AIRPORT_TYPES:
                continue
            ident = (row.get("ident") or "").upper().strip()
            if not ident:
                continue
            # Drop OurAirports placeholder idents (US-####) — these are
            # unregistered private strips that aren't depicted on sectionals.
            if ident.startswith("US-"):
                continue
            try:
                lat = float(row["latitude_deg"])
                lon = float(row["longitude_deg"])
            except (KeyError, ValueError):
                continue
            try:
                elev = float(row.get("elevation_ft") or 0)
            except ValueError:
                elev = 0.0
            out[ident] = {"lat": lat, "lon": lon, "elev_ft": int(elev)}
    return out


# ---------------------------------------------------------------------------
# Cell <-> airport mapping
# ---------------------------------------------------------------------------

def cells_within_radius(lat: float, lon: float,
                        radius_nm: float = LOW_ALT_RADIUS_NM
                        ) -> list[tuple[int, int]]:
    """Integer-cell coords (floor(lat), floor(lon)) whose 1°x1° tile lies within
    `radius_nm` of (lat, lon). Returns up to 4 cells for an airport near a
    cell corner.
    """
    delta_lat = radius_nm / NM_PER_DEG_LAT
    cos_lat = max(0.05, math.cos(math.radians(lat)))  # avoid /0 at poles
    delta_lon = radius_nm / (NM_PER_DEG_LAT * cos_lat)
    lat_lo, lat_hi = math.floor(lat - delta_lat), math.floor(lat + delta_lat)
    lon_lo, lon_hi = math.floor(lon - delta_lon), math.floor(lon + delta_lon)
    return [(la, lo)
            for la in range(lat_lo, lat_hi + 1)
            for lo in range(lon_lo, lon_hi + 1)]


def lat_band_of(lat: float, band_size: int = 1) -> int:
    """Return the lat-band integer for `lat` (floor to multiple of band_size)."""
    return math.floor(lat / band_size) * band_size


# ---------------------------------------------------------------------------
# Cell shard reading (no airport-specific altitude filter)
# ---------------------------------------------------------------------------

def read_cell_records(shard_gz: Path):
    """Yield records from a v2 grid cell shard. Sets `_alt_int` (int or None)
    so analyze_shard_quality can consume the records directly. Skips records
    missing hex/now/lat/lon since none of the quality metrics can use them.
    """
    if not shard_gz.exists():
        return
    try:
        with gzip.open(shard_gz, "rb") as f:
            for line in f:
                try:
                    record = _json.loads(line)
                except ValueError:
                    continue
                if (record.get("hex") is None
                        or record.get("now") is None
                        or record.get("lat") is None
                        or record.get("lon") is None):
                    continue
                alt = record.get("alt_baro")
                alt_int = None
                if alt is not None and alt != "ground":
                    try:
                        alt_int = int(alt)
                    except (ValueError, TypeError):
                        pass
                record["_alt_int"] = alt_int
                yield record
    except (EOFError, OSError) as e:
        logger.warning(f"Error reading {shard_gz}: {e} (using partial data)")


def cell_shard_path(grid_dir: Path, date_tag: str,
                    cell: tuple[int, int]) -> Path:
    lat, lon = cell
    return grid_dir / date_tag / f"{date_tag}_{lat}_{lon}.gz"


def aq_day_path(aq_dir: Path, date_tag: str) -> Path:
    """Per-day raw-results JSON path."""
    return aq_dir / f"{date_tag}.json"


# ---------------------------------------------------------------------------
# Per-date worker
# ---------------------------------------------------------------------------

def _compute_one_date(grid_dir: Path, airports: dict, airport_to_cells: dict,
                      airports_by_band: dict, date: datetime.date,
                      verbose: bool, aq_dir: Path | None = None,
                      skip_existing: bool = True) -> tuple[str, dict]:
    """Compute per-airport quality results for ONE date.

    If `aq_dir` is given and the day file already exists, load and return it
    (skip_existing=True). Otherwise compute, write the file, and return.
    Returns (date_tag, {icao: per_date_result_or_None}). Pure function — no
    shared mutable state — so it's safe to call concurrently from threads.
    """
    import time
    date_tag = date.strftime("%Y%m%d")

    # Cache check: if the per-day file already exists, just read it.
    if aq_dir is not None and skip_existing:
        day_path = aq_day_path(aq_dir, date_tag)
        if day_path.exists():
            with open(day_path, "r", encoding="utf-8") as f:
                cached = json.load(f)
            logger.info(f"  using cached {day_path} ({len(cached)} airports)")
            return date_tag, cached

    out: dict[str, dict | None] = {}

    for band, icaos in sorted(airports_by_band.items()):
        t_band = time.time()
        band_cells: set[tuple[int, int]] = set()
        for icao in icaos:
            band_cells |= airport_to_cells[icao]

        # Decode band's cells.
        t_decode = time.time()
        cell_records: dict[tuple[int, int], list[dict]] = {}
        cells_present = 0
        records_total = 0
        for cell in band_cells:
            path = cell_shard_path(grid_dir, date_tag, cell)
            if path.exists():
                cell_records[cell] = list(read_cell_records(path))
                cells_present += 1
                records_total += len(cell_records[cell])
        decode_s = time.time() - t_decode

        # Score airports in this band.
        t_score = time.time()
        n_scored = n_no_records = 0
        for icao in icaos:
            info = airports[icao]
            records: list[dict] = []
            for cell in airport_to_cells[icao]:
                if cell in cell_records:
                    records.extend(cell_records[cell])
            if not records:
                out[icao] = None
                n_no_records += 1
                continue
            per_date = analyze_shard_quality(
                None,
                field_elev=info["elev_ft"],
                airport_lat=info["lat"],
                airport_lon=info["lon"],
                records=records,
            )
            if verbose:
                logger.info(f"  {icao} {date_tag}: {per_date}")
            out[icao] = per_date
            n_scored += 1
        score_s = time.time() - t_score
        band_s = time.time() - t_band

        # Estimate decoded-cell working set for memory tracking. Each parsed
        # record is roughly ~250 B in Python — close enough to flag spikes.
        est_mb = records_total * 250 / 1e6
        logger.info(
            f"  band {band:>3} {date_tag}: "
            f"{len(icaos):>4} airports, {cells_present:>3}/{len(band_cells):<3} cells, "
            f"{records_total:>8,} records (~{est_mb:>6.0f} MB), "
            f"decode {decode_s:>5.1f}s + score {score_s:>5.1f}s = {band_s:>5.1f}s "
            f"({n_scored} scored, {n_no_records} no-records)"
        )

        cell_records.clear()

    # Persist per-day results so a long multi-day run can be resumed and the
    # heavy compute step can be amortized across multiple aggregation runs.
    if aq_dir is not None:
        day_path = aq_day_path(aq_dir, date_tag)
        day_path.parent.mkdir(parents=True, exist_ok=True)
        # Write atomically (rename) so a crash mid-write doesn't leave a
        # partial file that the next run would skip.
        tmp = day_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(out, f)
        tmp.replace(day_path)
        logger.info(f"  wrote {day_path} ({len(out)} airports)")

    return date_tag, out


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def compute_days(
    grid_dir: Path,
    airports: dict[str, dict],
    date_range: list[datetime.date],
    aq_dir: Path = V2_AQ_DIR,
    skip_existing: bool = True,
    only_airport: str | None = None,
    verbose: bool = False,
    workers: int = 1,
    band_size: int = 1,
) -> dict[str, dict]:
    """Stage 1: compute per-day raw quality results and persist to disk.

    For each date in `date_range`, write `aq_dir/<YYYYMMDD>.json` containing
    `{icao: per_date_raw_dict_or_None}`. Returns the in-memory dict
    `{date_tag: {icao: per_date}}` for the dates processed. With
    skip_existing=True, any day whose file already exists is loaded from
    disk rather than recomputed.

    When `only_airport` is set, results are NOT written to disk — debugging
    runs shouldn't pollute the cache (a partial-airport-set file would
    masquerade as a full-airport-set cache hit on later runs).

    `band_size` bounds peak in-memory decoded cells. Smaller = lower
    per-worker RAM at modest startup cost. Default 1° keeps worst-case
    ~7-8 GB/worker for populated CONUS bands.
    """
    write_dir: Path | None = aq_dir
    if only_airport:
        only_airport = only_airport.upper()
        if only_airport not in airports:
            raise KeyError(f"Airport {only_airport} not in airport set")
        airports = {only_airport: airports[only_airport]}
        write_dir = None        # don't poison the cache from a partial run

    airport_to_cells = {icao: set(cells_within_radius(info["lat"], info["lon"]))
                        for icao, info in airports.items()}

    airports_by_band: dict[int, list[str]] = defaultdict(list)
    for icao, info in airports.items():
        airports_by_band[lat_band_of(info["lat"], band_size)].append(icao)

    per_day: dict[str, dict] = {}

    def _run_date(date):
        return _compute_one_date(grid_dir, airports, airport_to_cells,
                                 airports_by_band, date, verbose,
                                 aq_dir=write_dir, skip_existing=skip_existing)

    if workers > 1 and len(date_range) > 1:
        # Threading is a fine fit: the inner loop is dominated by gzip decode
        # (C-level, releases the GIL) and disk I/O. Sidesteps the macOS
        # multiprocessing-spawn quirks that bit us with mp.Pool.
        from concurrent.futures import ThreadPoolExecutor, as_completed
        n_workers = min(workers, len(date_range))
        logger.info(f"using {n_workers} threads across {len(date_range)} dates")
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            futs = [pool.submit(_run_date, d) for d in date_range]
            for fut in as_completed(futs):
                date_tag, day_results = fut.result()
                per_day[date_tag] = day_results
    else:
        for date in date_range:
            date_tag, day_results = _run_date(date)
            per_day[date_tag] = day_results

    return per_day


def aggregate_days(
    date_range: list[datetime.date],
    aq_dir: Path = V2_AQ_DIR,
    require_all: bool = False,
) -> dict[str, dict]:
    """Stage 2: aggregate per-day raw results into per-airport quality dicts.

    Reads `aq_dir/<YYYYMMDD>.json` for each date in `date_range` and runs
    `aggregate_per_date_results` per airport. Missing day files are silently
    skipped unless `require_all=True`, in which case a FileNotFoundError is
    raised. Returns the same {ICAO: quality_dict} shape as the legacy
    build_v2_airport_quality.
    """
    per_date_results: dict[str, list[dict | None]] = defaultdict(list)
    found_days = 0
    for date in date_range:
        date_tag = date.strftime("%Y%m%d")
        day_path = aq_day_path(aq_dir, date_tag)
        if not day_path.exists():
            if require_all:
                raise FileNotFoundError(f"missing per-day file: {day_path}")
            logger.warning(f"  missing per-day file: {day_path} (skipping)")
            continue
        with open(day_path, "r", encoding="utf-8") as f:
            day = json.load(f)
        found_days += 1
        for icao, per_date in day.items():
            per_date_results[icao].append(per_date)

    logger.info(f"aggregating {found_days} days × {len(per_date_results)} airports")

    final: dict[str, dict] = {}
    num_dates = len(date_range)
    for icao, results in per_date_results.items():
        if not any(r is not None for r in results):
            # Evaluated but no records — keep so the map renders a grey "?".
            pass
        final[icao] = aggregate_per_date_results(results, icao,
                                                 num_dates=num_dates)
    return final


def build_v2_airport_quality(
    grid_dir: Path,
    airports: dict[str, dict],
    date_range: list[datetime.date],
    aq_dir: Path = V2_AQ_DIR,
    only_airport: str | None = None,
    verbose: bool = False,
    workers: int = 1,
    band_size: int = 1,
    skip_existing: bool = True,
) -> dict[str, dict]:
    """End-to-end: compute any missing per-day files, then aggregate.

    Equivalent to compute_days() followed by aggregate_days() — preserves
    the legacy single-call API for callers (e.g. pipeline.py) that don't
    care about the two-stage split.
    """
    # Stage 1: compute (or load cached) per-day files. compute_days handles
    # only_airport itself (and disables cache writes when set), so we pass it
    # through rather than pre-filtering.
    per_day = compute_days(grid_dir=grid_dir, airports=airports,
                           date_range=date_range, aq_dir=aq_dir,
                           skip_existing=skip_existing,
                           only_airport=only_airport,
                           verbose=verbose, workers=workers,
                           band_size=band_size)

    # Apply only_airport filter to the in-memory airport set for the
    # aggregation step too.
    if only_airport:
        only_airport = only_airport.upper()
        airports = {only_airport: airports[only_airport]}

    # Stage 2: aggregate the in-memory per-day dicts. Walk these directly
    # (rather than re-reading the on-disk files) so only_airport filtering
    # is honored and we save an I/O round-trip in the common case.
    airport_to_cells = {icao: set(cells_within_radius(info["lat"], info["lon"]))
                        for icao, info in airports.items()}

    per_date_results: dict[str, list[dict | None]] = defaultdict(list)
    for date in date_range:
        date_tag = date.strftime("%Y%m%d")
        day = per_day.get(date_tag, {})
        for icao, per_date in day.items():
            per_date_results[icao].append(per_date)

    num_dates = len(date_range)
    final: dict[str, dict] = {}
    for icao, results in per_date_results.items():
        # Skip airports that had no records on any date AND aren't in scope
        # (no nearby cell file ever existed). In-scope-but-no-records keep
        # an entry so the map renders a grey "?".
        if not any(r is not None for r in results):
            in_scope = False
            for date in date_range:
                date_tag = date.strftime("%Y%m%d")
                for cell in airport_to_cells.get(icao, ()):
                    if cell_shard_path(grid_dir, date_tag, cell).exists():
                        in_scope = True
                        break
                if in_scope:
                    break
            if not in_scope:
                continue
        final[icao] = aggregate_per_date_results(results, icao,
                                                 num_dates=num_dates)
    return final


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_date(s: str) -> datetime.date:
    return datetime.datetime.strptime(s, "%Y%m%d").date()


def _date_range(start: datetime.date, end: datetime.date) -> list[datetime.date]:
    out = []
    d = start
    while d <= end:
        out.append(d)
        d += datetime.timedelta(days=1)
    return out


def main():
    parser = argparse.ArgumentParser(
        description="Compute v2 per-airport ADS-B data quality.")
    parser.add_argument("--mode", choices=["compute", "aggregate", "all"],
                        default="all",
                        help="compute: stage 1 only (per-day files in --aq-dir). "
                             "aggregate: stage 2 only (combine existing per-day "
                             "files into --output). all: both (default).")
    parser.add_argument("--grid-dir", type=Path, default=V2_GRID_DIR,
                        help="v2 grid directory (default: data/v2/grid)")
    parser.add_argument("--aq-dir", type=Path, default=V2_AQ_DIR,
                        help="Per-day raw quality JSON directory "
                             f"(default: {V2_AQ_DIR})")
    parser.add_argument("--start-date", required=True,
                        help="Start date YYYYMMDD")
    parser.add_argument("--end-date", required=True,
                        help="End date YYYYMMDD (inclusive)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help="Aggregated output JSON path "
                             f"(default: {DEFAULT_OUTPUT}). Ignored in --mode compute.")
    parser.add_argument("--airport", type=str, default=None,
                        help="Restrict to a single ICAO (debugging).")
    parser.add_argument("--workers", type=int, default=1,
                        help="Parallel workers (one date per worker). Default 1.")
    parser.add_argument("--force", action="store_true",
                        help="Recompute per-day files even if they already exist.")
    parser.add_argument("--verbose", action="store_true",
                        help="Log per-airport, per-date metrics.")
    args = parser.parse_args()

    # INFO is the default — per-band stats are useful in normal runs.
    # --verbose flips on additional per-airport, per-date detail (added by
    # the inner loop).
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
    )

    start = _parse_date(args.start_date)
    end = _parse_date(args.end_date)
    if end < start:
        parser.error("--end-date must be >= --start-date")
    dates = _date_range(start, end)

    if args.mode in ("compute", "all"):
        airports = load_us_airports_with_elev()
        print(f"Loaded {len(airports)} US airports.", file=sys.stderr)

        per_day = compute_days(
            grid_dir=args.grid_dir,
            airports=airports,
            date_range=dates,
            aq_dir=args.aq_dir,
            skip_existing=not args.force,
            only_airport=args.airport,
            verbose=args.verbose,
            workers=args.workers,
        )
        print(f"Stage 1: {len(per_day)} day(s) computed/cached in {args.aq_dir}",
              file=sys.stderr)

    if args.mode == "compute":
        return

    # Aggregate stage.
    result = aggregate_days(date_range=dates, aq_dir=args.aq_dir)
    if args.airport:
        only = args.airport.upper()
        result = {k: v for k, v in result.items() if k == only}

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f)
    print(f"Wrote {len(result)} airport entries → {args.output}",
          file=sys.stderr)


if __name__ == "__main__":
    main()
