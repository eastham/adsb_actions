"""Data quality assessment from ADS-B shards.

Analyzes receiver coverage quality by detecting low-altitude tracks that
disappear before reaching the surface, and measuring inter-point time
gaps.  Produces a green/yellow/red score for the visualizer.

Used by batch_los_pipeline.py during the aggregation phase.
"""

import gzip
import json
import logging
import math
from collections import defaultdict
from pathlib import Path

try:
    from src.tools.busyness import read_shard_records, parse_date_from_shard
    from src.tools.batch_helpers import FT_MAX_ABOVE_AIRPORT, FT_MIN_BELOW_AIRPORT
except ImportError:
    from busyness import read_shard_records, parse_date_from_shard
    from batch_helpers import FT_MAX_ABOVE_AIRPORT, FT_MIN_BELOW_AIRPORT

logger = logging.getLogger(__name__)

# Low-altitude corridor for track-loss metric (feet AGL)
LOW_ALT_MIN_AGL = 800
LOW_ALT_MAX_AGL = 1500
LOW_ALT_RADIUS_NM = 5
SURFACE_AGL = 200             # Below this = track reached near the surface

# Gap metric
MAX_TRACK_GAP_S = 300         # Gaps above this excluded from gap statistics

# Scoring thresholds
TERM_GREEN = 0.25
TERM_YELLOW = 0.50
GAP_GREEN = 5.0
GAP_YELLOW = 15.0
DAYS_GREEN = 5
DAYS_YELLOW = 3

# Nautical miles per degree of latitude
NM_PER_DEG_LAT = 60.0


def _fast_distance_nm(lat1, lon1, lat2, lon2):
    """Fast approximate distance in nautical miles (equirectangular)."""
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    avg_lat = (lat1 + lat2) / 2.0
    dlon_adjusted = dlon * math.cos(math.radians(avg_lat))
    dist_deg = math.sqrt(dlat * dlat + dlon_adjusted * dlon_adjusted)
    return dist_deg * NM_PER_DEG_LAT


def analyze_shard_quality(shard_gz: Path, field_elev: int = 0,
                          airport_lat: float = 0.0,
                          airport_lon: float = 0.0) -> dict | None:
    """Analyze data quality metrics from a single gzipped JSONL shard.

    Returns dict with:
        lost_rate: float (0.0-1.0) or None if no low-altitude tracks found
        low_alt_tracks: int (total seen in the corridor)
        completed_tracks: int (also seen at the surface)
        lost_tracks: int (never seen at the surface)
        median_gap_s: float or None
        p90_gap_s: float or None
        total_tracks: int
        total_gaps: int
    Or None if the shard cannot be read or has no data.
    """
    low_alt_min = field_elev + LOW_ALT_MIN_AGL
    low_alt_max = field_elev + LOW_ALT_MAX_AGL
    surface_alt = field_elev + SURFACE_AGL

    # Group records by aircraft
    by_hex: dict[str, list[dict]] = defaultdict(list)

    for record in read_shard_records(shard_gz, field_elev):
        hex_id = record["hex"]
        alt_int = record.get("_alt_int")
        ts = record["now"]
        lat = record.get("lat")
        lon = record.get("lon")
        if lat is None or lon is None:
            continue
        by_hex[hex_id].append({"now": ts, "alt": alt_int, "lat": lat, "lon": lon})

    if not by_hex:
        return None

    completed = 0
    lost = 0
    all_gaps = []

    for hex_id, pts in by_hex.items():
        if len(pts) < 3:
            continue

        pts.sort(key=lambda x: x["now"])

        # Compute inter-point gaps (for gap metric, all tracks)
        for i in range(1, len(pts)):
            gap = pts[i]["now"] - pts[i - 1]["now"]
            if 0 < gap <= MAX_TRACK_GAP_S:
                all_gaps.append(gap)

        # Track-loss metric: check if any point is in the low-altitude
        # corridor (within 5nm, 800-1500ft AGL)
        in_low_alt = False
        reached_surface = False
        for p in pts:
            alt = p["alt"]
            if alt is None:
                continue
            dist = _fast_distance_nm(airport_lat, airport_lon,
                                     p["lat"], p["lon"])
            if (dist <= LOW_ALT_RADIUS_NM
                    and low_alt_min <= alt <= low_alt_max):
                in_low_alt = True
            if alt <= surface_alt:
                reached_surface = True

        if in_low_alt:
            if reached_surface:
                completed += 1
            else:
                lost += 1

    total_low = completed + lost
    lost_rate = (lost / total_low) if total_low > 0 else None

    all_gaps.sort()
    median_gap = all_gaps[len(all_gaps) // 2] if all_gaps else None
    p90_idx = int(len(all_gaps) * 0.9)
    p90_gap = all_gaps[p90_idx] if all_gaps else None

    return {
        "lost_rate": lost_rate,
        "low_alt_tracks": total_low,
        "completed_tracks": completed,
        "lost_tracks": lost,
        "median_gap_s": median_gap,
        "p90_gap_s": p90_gap,
        "total_tracks": len(by_hex),
        "total_gaps": len(all_gaps),
    }


def build_data_quality(icao: str, airport_dir: Path,
                       field_elev: int = 0,
                       airport_lat: float = 0.0,
                       airport_lon: float = 0.0) -> dict | None:
    """Build data quality JSON structure for one airport across all dates.

    Returns a dict suitable for JSON embedding in the HTML:
        {
            "icao": str,
            "score": "green" | "yellow" | "red",
            "lostRate": float or None,
            "completionRate": float or None,
            "medianGapS": float or None,
            "p90GapS": float or None,
            "numDates": int,
            "totalLowAltTracks": int,
            "completedTracks": int,
            "details": {
                "terminationScore": "green"|"yellow"|"red",
                "gapScore": "green"|"yellow"|"red",
                "confidenceScore": "green"|"yellow"|"red"
            }
        }
    Or None if no data is available.
    """
    shard_files = sorted(airport_dir.glob("*_*.gz"))
    shard_files = [f for f in shard_files if parse_date_from_shard(f.name)]

    if not shard_files:
        return None

    all_results = []
    for shard in shard_files:
        result = analyze_shard_quality(shard, field_elev=field_elev,
                                       airport_lat=airport_lat,
                                       airport_lon=airport_lon)
        if result:
            all_results.append(result)

    if not all_results:
        return None

    # Weighted average lost rate
    total_low = sum(r["low_alt_tracks"] for r in all_results)
    total_completed = sum(r["completed_tracks"] for r in all_results)
    weighted_lost_rate = None
    completion_rate = None
    if total_low > 0:
        weighted_lost_rate = 1.0 - (total_completed / total_low)
        completion_rate = total_completed / total_low

    # Aggregate gap metrics (median of medians)
    median_gaps = [r["median_gap_s"] for r in all_results
                   if r["median_gap_s"] is not None]
    median_gaps.sort()
    agg_median_gap = median_gaps[len(median_gaps) // 2] if median_gaps else None

    p90_gaps = [r["p90_gap_s"] for r in all_results
                if r["p90_gap_s"] is not None]
    p90_gaps.sort()
    agg_p90_gap = p90_gaps[len(p90_gaps) // 2] if p90_gaps else None

    num_dates = len(shard_files)

    term_score = _score_termination(weighted_lost_rate)
    gap_score = _score_gap(agg_median_gap)
    conf_score = _score_confidence(num_dates)
    overall = _overall_score(term_score, gap_score, conf_score)

    result = {
        "icao": icao,
        "score": overall,
        "lostRate": round(weighted_lost_rate, 3) if weighted_lost_rate is not None else None,
        "completionRate": round(completion_rate, 3) if completion_rate is not None else None,
        "medianGapS": round(agg_median_gap, 1) if agg_median_gap is not None else None,
        "p90GapS": round(agg_p90_gap, 1) if agg_p90_gap is not None else None,
        "numDates": num_dates,
        "totalLowAltTracks": total_low,
        "completedTracks": total_completed,
        "details": {
            "terminationScore": term_score,
            "gapScore": gap_score,
            "confidenceScore": conf_score,
        },
    }

    logger.info(
        f"Data quality for {icao}: score={overall}, "
        f"lost={weighted_lost_rate}, gap={agg_median_gap}s, "
        f"{num_dates} dates, {total_low} low-alt tracks"
    )

    return result


def _score_termination(rate: float | None) -> str:
    if rate is None:
        return "yellow"
    if rate < TERM_GREEN:
        return "green"
    if rate < TERM_YELLOW:
        return "yellow"
    return "red"


def _score_gap(median_gap: float | None) -> str:
    if median_gap is None:
        return "yellow"
    if median_gap < GAP_GREEN:
        return "green"
    if median_gap < GAP_YELLOW:
        return "yellow"
    return "red"


def _score_confidence(num_dates: int) -> str:
    if num_dates >= DAYS_GREEN:
        return "green"
    if num_dates >= DAYS_YELLOW:
        return "yellow"
    return "red"


_SCORE_ORDER = {"green": 0, "yellow": 1, "red": 2}


def _overall_score(term_score: str, gap_score: str, conf_score: str) -> str:
    worst = max(term_score, gap_score, conf_score,
                key=lambda s: _SCORE_ORDER[s])
    return worst
