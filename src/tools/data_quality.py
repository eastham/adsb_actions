"""Data quality assessment from ADS-B shards.

Analyzes receiver coverage quality by detecting low-altitude tracks that
disappear before reaching the surface, and measuring inter-point time
gaps.  Produces a green/yellow/red score for the visualizer.

Also measures runway usage (runway_votes_for_track)

Used by batch_los_pipeline.py during the aggregation phase.
"""

import logging
import math
from collections import defaultdict
from pathlib import Path

try:
    from src.tools.busyness import read_shard_records, parse_date_from_shard
    from src.tools.runway_usage import build_runway_boxes, runway_votes_for_track
except ImportError:
    from busyness import read_shard_records, parse_date_from_shard
    from runway_usage import build_runway_boxes, runway_votes_for_track

logger = logging.getLogger(__name__)

# Low-altitude corridor for track-loss metric (feet AGL)
LOW_ALT_MIN_AGL = 800
LOW_ALT_MAX_AGL = 1500
LOW_ALT_RADIUS_NM = 5
SURFACE_AGL = 500             # Below this AGL inside the ring = "completed
                              # approach". 
                              
# A track that enters AND exits the LOW_ALT_RADIUS_NM ring while still
# being tracked (≥2 boundary transitions) is treated as a transit, not an
# approach to this airport, and dropped from both numerator and denominator
# of the track-loss metric. The argument: if the receiver kept the aircraft
# the whole way through the ring, it can't have been "lost" in any
# coverage-meaningful sense — whatever the pilot was doing.
# (No altitude floor — missed approaches with full ring-exit visibility
# also get dropped, which is the right call for a receiver-coverage metric.)

# Gap metric
MAX_TRACK_GAP_S = 300         # Gaps above this excluded from gap statistics

# Scoring thresholds — overall score is the worst of termination and gap scores
TERM_GREEN = 0.25             # track-loss rate below this → green
TERM_YELLOW = 0.50            # below this → yellow, above → red
GAP_GREEN = 5.0               # median gap (seconds) below this → green
GAP_YELLOW = 15.0             # below this → yellow, above → red

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


def _summarize_runway_usage(runway_counts):
    """Turn {ident: count} into a list of {"runway", "pct"} sorted by pct desc.

    Percentages are whole numbers over the total votes; runways with zero
    votes are already absent. Returns [] when there are no votes.
    """
    total = sum(runway_counts.values())
    if total == 0:
        return []
    usage = [{"runway": ident, "pct": round(100 * n / total)}
             for ident, n in runway_counts.items()]
    usage.sort(key=lambda u: u["pct"], reverse=True)
    return usage


def analyze_shard_quality(shard_gz: Path, field_elev: int = 0,
                          airport_lat: float = 0.0,
                          airport_lon: float = 0.0,
                          records: list[dict] | None = None,
                          runway_boxes: list | None = None) -> dict | None:
    """Analyze data quality metrics from a single gzipped JSONL shard.

    If records is provided, uses those instead of reading from disk.
    If runway_boxes (from build_runway_boxes) is provided, each scored approach
    also votes for the runway whose approach box/centerline it lined up with.
    """
    low_alt_min = field_elev + LOW_ALT_MIN_AGL
    low_alt_max = field_elev + LOW_ALT_MAX_AGL
    surface_alt = field_elev + SURFACE_AGL

    # Group records by aircraft
    by_hex: dict[str, list[dict]] = defaultdict(list)

    for record in (records if records is not None
                   else read_shard_records(shard_gz, field_elev)):
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
    through = 0
    all_gaps = []
    runway_counts: dict[str, int] = defaultdict(int)

    for hex_id, pts in by_hex.items():
        if len(pts) < 3:
            continue

        pts.sort(key=lambda x: x["now"])

        # Compute inter-point gaps (for gap metric, all tracks)
        for i in range(1, len(pts)):
            gap = pts[i]["now"] - pts[i - 1]["now"]
            if 0 < gap <= MAX_TRACK_GAP_S:
                all_gaps.append(gap)

        # Track-loss metric, with through-flight and loiter detection.
        #
        # An approach we want to score: enters the 5 nm ring at corridor
        # altitude (800-1500 AGL → "in_corridor"), descends below the
        # corridor floor inside the ring (proves it was on a real final),
        # and either reaches surface (completed) or doesn't (lost).
        #
        # Dropped categories:
        # - Through-flight: enters AND exits the ring (≥2 transitions).
        #   Receiver kept tracking the whole transit, so it can't have
        #   been "lost" in any coverage-meaningful sense. Includes missed
        #   approaches with full ring-exit visibility — fine for a
        #   receiver-coverage metric.
        # - Loiter / pattern / overflight: track entered the corridor
        #   altitude band inside the ring but never descended below
        #   LOW_ALT_MIN_AGL (800 AGL). If the aircraft was actually on
        #   final it would cross that altitude on the way down. Tracks
        #   that stay above it are pattern work, surveying, helicopter
        #   loiter, or traffic to a *neighboring* airport that just
        #   clipped this airport's ring at altitude.
        in_corridor = False        # entered the 800-1500 AGL band inside ring
        descended_below_floor = False  # crossed below 800 AGL inside ring
        reached_surface = False
        ring_transitions = 0
        prev_inside: bool | None = None
        # Low, in-ring points fed to runway detection, ordered by time as
        # (now, lat, lon). We keep everything at or below the corridor top
        # (≤1500 AGL) inside the ring — pattern altitude and below, where final
        # happens. The approach-box geometry (extended centerline, see
        # _approach_runway) is what actually isolates final from downwind and
        # base; it doesn't need an altitude floor. Timestamps are kept so
        # runway_votes_for_track can split repeated pattern work into separate
        # approaches (on time gaps and on field overpasses).
        approach_pts = []
        for p in pts:
            alt = p["alt"]
            dist = _fast_distance_nm(airport_lat, airport_lon,
                                     p["lat"], p["lon"])
            inside = dist <= LOW_ALT_RADIUS_NM
            if prev_inside is not None and inside != prev_inside:
                ring_transitions += 1
            prev_inside = inside

            if alt is None:
                continue
            if inside and low_alt_min <= alt <= low_alt_max:
                in_corridor = True
            if inside and alt < low_alt_min:
                descended_below_floor = True
            if inside and alt <= low_alt_max:
                approach_pts.append((p["now"], p["lat"], p["lon"]))
            # Surface check is generous — any point ≤ surface_alt counts,
            # not just inside-ring. ADS-B sometimes drops out at very low
            # altitude inside the ring then re-acquires on the ramp at 0 ft
            # with lat/lon drift just outside; restricting to inside-ring
            # would undercount completions for those cases.
            if alt <= surface_alt:
                reached_surface = True

        # Runway usage is scored independently of the coverage classification
        # below. The classification drops several tracks that still contain
        # perfectly good approaches: a hex that leaves and re-enters the ring
        # (ring_transitions >= 2 → "through") because it flew multiple approaches
        # across the day, went around, or had an ADS-B dropout past 5nm; and a
        # low/fast pass that never descended below 800 AGL (not
        # descended_below_floor → "through"). Each of those approaches is exactly
        # what we want to count.
        # runway_votes_for_track splits the low/in-ring points into individual
        # approaches (on time gaps and field overpasses) and votes each one via
        # the approach-box geometry, which isolates final from downwind/base.
        if runway_boxes:
            for ident in runway_votes_for_track(approach_pts, runway_boxes):
                runway_counts[ident] += 1

        if in_corridor:
            if ring_transitions >= 2:
                through += 1
                continue
            if not descended_below_floor:
                # Never came below 800 AGL inside the ring — not an
                # approach attempt. Also dropped from the metric.
                through += 1
                continue
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
        "through_tracks": through,
        "median_gap_s": median_gap,
        "p90_gap_s": p90_gap,
        "total_tracks": len(by_hex),
        "total_gaps": len(all_gaps),
        "runway_counts": dict(runway_counts),
    }


def build_data_quality(icao: str, airport_dir: Path,
                       field_elev: int = 0,
                       airport_lat: float = 0.0,
                       airport_lon: float = 0.0,
                       preloaded_shards: dict[Path, list[dict]] | None = None
                       ) -> dict | None:
    """Build data quality JSON structure for one airport across all dates.

    If preloaded_shards is provided, uses those instead of reading from disk.
    Returns None when the airport has no shards at all (i.e. wasn't
    evaluated). When shards exist but yield no usable data, returns a dict
    with score="none" so callers can distinguish "evaluated, no data" from
    "not evaluated."
    """
    if preloaded_shards is not None:
        shard_files = sorted(preloaded_shards.keys())
    else:
        shard_files = sorted(airport_dir.glob("*_*.gz"))
        shard_files = [f for f in shard_files if parse_date_from_shard(f.name)]

    if not shard_files:
        return None

    # Load runway geometry once for the airport; empty list -> runway usage
    # simply isn't reported (e.g. airport not in OurAirports, or offline).
    runway_boxes = build_runway_boxes(icao)

    per_date_results = []
    for shard in shard_files:
        shard_records = preloaded_shards.get(shard) if preloaded_shards else None
        result = analyze_shard_quality(shard, field_elev=field_elev,
                                       airport_lat=airport_lat,
                                       airport_lon=airport_lon,
                                       records=shard_records,
                                       runway_boxes=runway_boxes)
        per_date_results.append(result)

    return aggregate_per_date_results(per_date_results, icao,
                                      num_dates=len(shard_files))


def aggregate_per_date_results(per_date: list[dict | None], icao: str,
                               num_dates: int | None = None) -> dict:
    """Combine per-date analyze_shard_quality results into one airport dict.

    `per_date` may contain Nones (dates with no records); they're filtered.
    `num_dates` defaults to len(per_date) if not given — pass it explicitly
    when the caller knows the intended date-range count (e.g. some dates had
    no shard at all and shouldn't have been in the list).

    Always returns a dict. When no per-date result has usable data, the
    returned dict has score="none" so callers can distinguish "evaluated,
    no data" from "not evaluated" (which a None return would mean).
    """
    if num_dates is None:
        num_dates = len(per_date)
    all_results = [r for r in per_date if r]

    total_low = sum(r["low_alt_tracks"] for r in all_results)
    total_completed = sum(r["completed_tracks"] for r in all_results)
    weighted_lost_rate = None
    completion_rate = None
    if total_low > 0:
        weighted_lost_rate = 1.0 - (total_completed / total_low)
        completion_rate = total_completed / total_low

    # Aggregate gap metrics (median of medians)
    median_gaps = sorted(r["median_gap_s"] for r in all_results
                         if r["median_gap_s"] is not None)
    agg_median_gap = median_gaps[len(median_gaps) // 2] if median_gaps else None

    p90_gaps = sorted(r["p90_gap_s"] for r in all_results
                      if r["p90_gap_s"] is not None)
    agg_p90_gap = p90_gaps[len(p90_gaps) // 2] if p90_gaps else None

    term_score = _score_termination(weighted_lost_rate)
    gap_score = _score_gap(agg_median_gap)
    overall = _overall_score(term_score, gap_score)

    # Aggregate runway-usage votes across dates into a percentage breakdown.
    runway_counts: dict[str, int] = defaultdict(int)
    for r in all_results:
        for ident, n in r.get("runway_counts", {}).items():
            runway_counts[ident] += n
    runway_usage = _summarize_runway_usage(runway_counts)

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
        "runwayUsage": runway_usage,
        "runwayCounts": dict(runway_counts),
        "details": {
            "terminationScore": term_score,
            "gapScore": gap_score,
        },
    }
    logger.debug(
        f"Data quality for {icao}: score={overall}, "
        f"lost={weighted_lost_rate}, gap={agg_median_gap}s, "
        f"{num_dates} dates, {total_low} low-alt tracks"
    )

    return result


def _score_termination(rate: float | None) -> str:
    if rate is None:
        return "none"
    if rate < TERM_GREEN:
        return "green"
    if rate < TERM_YELLOW:
        return "yellow"
    return "red"


def _score_gap(median_gap: float | None) -> str:
    if median_gap is None:
        return "none"
    if median_gap < GAP_GREEN:
        return "green"
    if median_gap < GAP_YELLOW:
        return "yellow"
    return "red"


_SCORE_ORDER = {"none": -1, "green": 0, "yellow": 1, "red": 2}


def _overall_score(term_score: str, gap_score: str) -> str:
    # Termination is the load-bearing signal (did approaches get tracked all
    # the way to the surface?). Without it we can't claim coverage is good
    # even if en-route gaps look fine — gap-only is a misleading green/yellow.
    # Gap=None alone is fine to defer to termination, since it implies near-
    # zero records and termination would also be None in that case.
    if term_score == "none":
        return "none"
    if gap_score == "none":
        return term_score
    return max(term_score, gap_score, key=lambda s: _SCORE_ORDER[s])
