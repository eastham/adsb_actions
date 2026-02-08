"""Busyness analysis: count aircraft per hour from ADS-B shards, join with
METAR flight categories, and produce aggregated JSON for the chart.

Used by batch_los_pipeline.py during the aggregation phase.
"""

import gzip
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

try:
    from src.tools.metar_history import get_flight_categories
    from src.tools.batch_helpers import FT_MAX_ABOVE_AIRPORT, FT_MIN_BELOW_AIRPORT
except ImportError:
    from metar_history import get_flight_categories
    from batch_helpers import FT_MAX_ABOVE_AIRPORT, FT_MIN_BELOW_AIRPORT

logger = logging.getLogger(__name__)


def read_shard_records(shard_gz: Path, field_elev: int = 0):
    """Yield filtered records from a gzipped JSONL shard.

    Filters to altitude band field_elev ± FT_MAX_ABOVE/FT_MIN_BELOW.
    Yields dicts with at least: hex, now, alt_baro (as int or None), lat, lon.
    Records missing hex or now are skipped.
    """
    alt_ceil = field_elev + FT_MAX_ABOVE_AIRPORT
    alt_floor = field_elev + FT_MIN_BELOW_AIRPORT

    try:
        with gzip.open(shard_gz, "rt") as f:
            for line in f:
                try:
                    record = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    continue

                ts = record.get("now")
                hex_id = record.get("hex")
                if ts is None or hex_id is None:
                    continue

                alt = record.get("alt_baro")
                alt_int = None
                if alt is not None and alt != "ground":
                    try:
                        alt_int = int(alt)
                    except (ValueError, TypeError):
                        pass
                    if alt_int is not None and (alt_int > alt_ceil
                                                or alt_int < alt_floor):
                        continue

                record["_alt_int"] = alt_int
                yield record
    except (EOFError, OSError) as e:
        logger.warning(f"Error reading {shard_gz}: {e} (using partial data)")


def count_hourly_traffic(shard_gz: Path,
                         field_elev: int = 0) -> dict[int, int]:
    """Count unique aircraft per UTC hour from a single gzipped JSONL shard.

    Filters to aircraft within FT_MAX_ABOVE_AIRPORT/FT_MIN_BELOW_AIRPORT of
    field_elev to exclude high-altitude overflights and ground targets.

    Returns {hour: unique_aircraft_count} for hours 0-23.
    """
    hex_by_hour: dict[int, set[str]] = defaultdict(set)

    for record in read_shard_records(shard_gz, field_elev):
        hour = datetime.fromtimestamp(record["now"], tz=timezone.utc).hour
        hex_by_hour[hour].add(record["hex"])

    return {h: len(hexes) for h, hexes in hex_by_hour.items()}


def parse_date_from_shard(filename: str) -> str | None:
    """Extract ISO date string from shard filename like '060125_KWVI.gz'.

    Returns '2025-06-01' or None if unparseable.
    """
    # Format: MMDDYY_ICAO.gz
    basename = Path(filename).stem  # '060125_KWVI'
    date_part = basename.split("_")[0]  # '060125'
    if len(date_part) != 6:
        return None
    try:
        mm, dd, yy = date_part[:2], date_part[2:4], date_part[4:6]
        return f"20{yy}-{mm}-{dd}"
    except (ValueError, IndexError):
        return None


def build_busyness_data(icao: str, airport_dir: Path,
                        metar_year: int = 2025,
                        metar_cache_dir: Path | None = None,
                        field_elev: int = 0
                        ) -> dict | None:
    """Build the full busyness JSON structure for one airport.

    Reads .gz shards for traffic counts, fetches/caches METAR data,
    joins them, and aggregates by (hour, day_type, flight_category).

    Returns a dict suitable for JSON embedding in the HTML chart, or None
    if no data is available.
    """
    # Find all shard files
    shard_files = sorted(airport_dir.glob("*_*.gz"))
    # Exclude any that don't match the date pattern
    shard_files = [f for f in shard_files if parse_date_from_shard(f.name)]

    if not shard_files:
        logger.warning(f"No shard files found for {icao}")
        return None

    # Get flight categories from METAR
    cache_dir = metar_cache_dir or airport_dir
    flight_cats = get_flight_categories(icao, metar_year, cache_dir)
    has_weather = flight_cats is not None and len(flight_cats) > 0

    if not has_weather:
        logger.info(f"No METAR data for {icao}, busyness chart will omit weather filter")

    # Count traffic per (date, hour) and determine day type
    # Accumulate into buckets: (hour, day_type, flight_cat) -> [count1, count2, ...]
    buckets: dict[tuple[int, str, str], list[int]] = defaultdict(list)

    for shard in shard_files:
        date_str = parse_date_from_shard(shard.name)
        if not date_str:
            continue

        hourly = count_hourly_traffic(shard, field_elev=field_elev)
        if not hourly:
            continue

        # Determine day of week
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            continue
        is_weekend = dt.weekday() >= 5  # Saturday=5, Sunday=6
        day_type = "weekend" if is_weekend else "weekday"

        for hour, count in hourly.items():
            if has_weather:
                cat = flight_cats.get((date_str, hour), "UNKNOWN")
                # Merge LIFR into IFR, then rename to pilot-facing terms
                _CAT_MAP = {"VFR": "VMC", "MVFR": "MVMC",
                            "IFR": "IMC", "LIFR": "IMC"}
                cat = _CAT_MAP.get(cat, cat)
            else:
                cat = "ALL"

            # Always add to the specific bucket
            if cat != "UNKNOWN":
                buckets[(hour, day_type, cat)].append(count)
            # Also add to "ALL" weather bucket
            buckets[(hour, day_type, "ALL")].append(count)
            # Also add to "all" day_type bucket
            if cat != "UNKNOWN":
                buckets[(hour, "all", cat)].append(count)
            buckets[(hour, "all", "ALL")].append(count)

    if not buckets:
        logger.warning(f"No traffic data generated for {icao}")
        return None

    # Compute averages and sample counts
    aggregated = {}
    global_max = 0.0
    for (hour, day_type, cat), counts in buckets.items():
        avg = sum(counts) / len(counts)
        key = f"{hour}:{day_type}:{cat}"
        aggregated[key] = {
            "avg": round(avg, 1),
            "n": len(counts),
        }
        if avg > global_max:
            global_max = avg

    # Determine which weather categories have data
    weather_cats = sorted(set(
        cat for (_, _, cat) in buckets.keys()
        if cat not in ("ALL", "UNKNOWN")
    ))

    result = {
        "data": aggregated,
        "globalMax": round(global_max, 1),
        "hasWeather": has_weather,
        "weatherCategories": weather_cats,
        "numDates": len(shard_files),
        "icao": icao,
    }

    logger.info(
        f"Busyness data for {icao}: {len(shard_files)} dates, "
        f"{len(aggregated)} buckets, globalMax={global_max:.1f}, "
        f"weather={'yes' if has_weather else 'no'}"
    )

    return result
