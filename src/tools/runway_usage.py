"""Runway-usage detection from ADS-B tracks.

Given an airport's low-altitude tracks, decide which runway each arriving
aircraft approached, so pilots know what to expect on arrival. Produces a
per-runway vote tally that data_quality.py turns into the "runwayUsage"
breakdown shown in the visualizer.

The hard part is telling *final approach* apart from downwind and base legs,
since a track can drop out (lose ADS-B coverage) on any leg. We can't just take
the last heading: downwind is the reciprocal of the landing direction and base
is ~90° across it, so trusting the last segment would vote for the wrong (often
reciprocal) runway.

Instead we gate on geometry. Each runway end has an "approach box": a rectangle
extending APPROACH_BOX_LEN_NM out from the landing threshold along the extended
centerline, APPROACH_BOX_WIDTH_FT wide. A track votes for a runway only when a
segment is (a) inside that runway's box — i.e. on the extended centerline, which
excludes downwind and base — AND (b) aligned with the runway heading within
RUNWAY_MATCH_TOLERANCE_DEG, sustained for at least MIN_FINAL_RUN_NM. A track
that dropped out before establishing on final matches no box and simply casts no
vote, which is better than a confidently wrong one.

Repeated pattern work (a training aircraft flying circuits) is split into
separate approaches so each one votes for the runway it actually flew rather
than collapsing into a single vote. We split two ways: on time gaps between
visits (APPROACH_SPLIT_GAP_S) and, within one continuous visit, at each field
overpass (CROSS_NEAR_NM / CROSS_FAR_NM) to separate back-to-back touch-and-goes.

Note on magnetic vs. true: runway *idents* (e.g. "27") are MAGNETIC, but
bearings from lat/lon and OurAirports' `_degT` headings are both TRUE. All
matching is done in true degrees; the ident is only a label.
"""

import logging
import math

try:
    from src.tools.generate_airport_config import load_runways
except ImportError:
    from generate_airport_config import load_runways

logger = logging.getLogger(__name__)

# Geometry constants
NM_PER_DEG_LAT = 60.0
FT_PER_NM = 6076.12

# Detection tuning
RUNWAY_MATCH_TOLERANCE_DEG = 20   # segment course must be within this of the
                                  # runway heading to count as on final. Tight
                                  # enough to reject base/crosswind legs that
                                  # clip the box but aren't lined up.
APPROACH_BOX_LEN_NM = 1.5         # box extends this far out from the threshold.
                                  # Kept short (final, not pattern): a longer
                                  # box catches 45° downwind-entry legs that
                                  # merely clip the extended centerline.
APPROACH_BOX_WIDTH_FT = 2500      # total box width across the centerline
MIN_FINAL_RUN_NM = 0.2            # a track must track a runway's centerline,
                                  # aligned and continuous, for at least this
                                  # far to vote. Rejects tight-pattern turns
                                  # that momentarily clip a box mid-turn but
                                  # actually land on a different runway.
APPROACH_SPLIT_GAP_S = 600        # a time gap larger than this (10 min) between
                                  # a hex's points starts a new approach for
                                  # runway voting. One aircraft doing pattern
                                  # work all day would otherwise collapse into a
                                  # single vote; splitting lets each visit vote
                                  # for the runway it actually flew.
# A continuously-tracked touch-and-go (approach, overfly the field, climb out,
# re-enter the pattern, approach again) has no time gap to split on, so it too
# would collapse into one vote. We also cut at each field overpass: the track
# passes within CROSS_NEAR_NM of the airport, then must travel back out beyond
# CROSS_FAR_NM before the next overpass counts (hysteresis, so GPS jitter near
# the threshold doesn't split a single approach).
CROSS_NEAR_NM = 0.4               # within this of the airport = an overpass
CROSS_FAR_NM = 1.0                # must exit past this before another overpass


def _fast_distance_nm(lat1, lon1, lat2, lon2):
    """Fast approximate distance in nautical miles (equirectangular)."""
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    avg_lat = (lat1 + lat2) / 2.0
    dlon_adjusted = dlon * math.cos(math.radians(avg_lat))
    dist_deg = math.sqrt(dlat * dlat + dlon_adjusted * dlon_adjusted)
    return dist_deg * NM_PER_DEG_LAT


def _bearing_deg(lat1, lon1, lat2, lon2):
    """Initial great-circle bearing (degrees true, 0-360) from pt1 to pt2."""
    lat1r = math.radians(lat1)
    lat2r = math.radians(lat2)
    dlon = math.radians(lon2 - lon1)
    y = math.sin(dlon) * math.cos(lat2r)
    x = (math.cos(lat1r) * math.sin(lat2r)
         - math.sin(lat1r) * math.cos(lat2r) * math.cos(dlon))
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


def _circular_diff(a, b):
    """Smallest absolute difference between two headings in degrees (0-180)."""
    d = abs(a - b) % 360.0
    return min(d, 360.0 - d)


def _normalize_runway_ident(ident):
    """Collapse a runway ident to its bare two-digit number, or "" if invalid.

    Parallel runways (09L/09R) share a heading and can't be distinguished from
    approach course alone, so we report on runway "09" rather than guess a
    side: the L/R/C side suffix is dropped. The number is zero-padded to two
    digits so "9" and "09" don't become separate runways in the tally.

    Returns "" for anything that isn't a real runway number 01-36 — helipads
    ("H1"), water lanes, and compass-named grass strips ("N", "NE", "SWL")
    that OurAirports also lists as runway ends. Callers skip empty idents, so
    these never get an approach box or collect votes.
    """
    ident = ident.strip().upper()
    if ident and ident[-1] in ("L", "R", "C"):
        ident = ident[:-1]
    if not ident.isdigit():
        return ""
    num = int(ident)
    if not 1 <= num <= 36:
        return ""
    return f"{num:02d}"


def build_runway_boxes(icao):
    """Return approach-box descriptors for each runway end of an airport.

    Each descriptor is a dict:
        ident:    normalized runway number (e.g. "20")
        heading:  landing heading, degrees true
        lat, lon: landing threshold coordinates
    Both ends of each runway are emitted (you can land either direction).
    Reads OurAirports runway data via load_runways(). Closed runways, ends
    with no threshold coordinates, and ends with no usable heading are skipped.
    When `_degT` is blank the heading is derived from the two threshold
    coordinates (still true). Deduplicated by normalized ident.
    """
    try:
        rows = load_runways(icao)
    except Exception as e:
        logger.warning(f"Could not load runways for {icao}: {e}")
        return []

    boxes = {}
    for row in rows:
        if row.get("closed", "").strip() == "1":
            continue
        # Each OurAirports row is one physical runway with two ends: "le" (low)
        # and "he" (high), e.g. 09/27. Emit a box for both, since you can land
        # either direction. `prefix` is the end we're building; `other` is the
        # opposite end, used to derive a heading when this end's `_degT` is
        # blank (bearing from this threshold toward the other one).
        for prefix, other in (("le", "he"), ("he", "le")):
            ident = row.get(f"{prefix}_ident", "").strip()
            if not ident:
                continue
            try:
                tlat = float(row[f"{prefix}_latitude_deg"])
                tlon = float(row[f"{prefix}_longitude_deg"])
            except (KeyError, ValueError):
                continue
            heading = _runway_end_heading(row, prefix, other, tlat, tlon)
            if heading is None:
                continue
            # norm is "" for helipads / compass strips / out-of-range numbers
            # (see _normalize_runway_ident) — skip those. setdefault keeps the
            # first threshold seen for a normalized ident; parallels (09L/09R)
            # collapse to one box, which is intended since we can't tell the
            # side from approach course anyway.
            norm = _normalize_runway_ident(ident)
            if norm:
                boxes.setdefault(norm, {"ident": norm, "heading": heading,
                                        "lat": tlat, "lon": tlon})
    return list(boxes.values())


def _runway_end_heading(row, prefix, other, tlat, tlon):
    """True landing heading for one runway end; None if undeterminable.

    Prefers the `_degT` column; falls back to the bearing from this end's
    threshold to the opposite end's threshold.
    """
    raw = row.get(f"{prefix}_heading_degT", "").strip()
    if raw:
        try:
            return float(raw) % 360.0
        except ValueError:
            pass
    try:
        olat = float(row[f"{other}_latitude_deg"])
        olon = float(row[f"{other}_longitude_deg"])
    except (KeyError, ValueError):
        return None
    return _bearing_deg(tlat, tlon, olat, olon)


def _point_in_approach_box(lat, lon, box):
    """Is (lat, lon) within a runway's approach box (extended centerline)?

    The box starts at the landing threshold and extends APPROACH_BOX_LEN_NM
    outward along the *reciprocal* of the landing heading (the direction final
    traffic comes from), APPROACH_BOX_WIDTH_FT wide across the centerline.
    Uses a local equirectangular projection — fine at ≤3 nm.
    """
    dlat_nm = (lat - box["lat"]) * NM_PER_DEG_LAT
    dlon_nm = ((lon - box["lon"]) * NM_PER_DEG_LAT
               * math.cos(math.radians(box["lat"])))
    # Approach axis points outward from the threshold: reciprocal of landing.
    approach = math.radians((box["heading"] + 180.0) % 360.0)
    axis_n, axis_e = math.cos(approach), math.sin(approach)
    along = dlat_nm * axis_n + dlon_nm * axis_e        # distance out on final
    cross = -dlat_nm * axis_e + dlon_nm * axis_n       # offset from centerline
    half_width_nm = (APPROACH_BOX_WIDTH_FT / 2.0) / FT_PER_NM
    return 0.0 <= along <= APPROACH_BOX_LEN_NM and abs(cross) <= half_width_nm


def _longest_final_run_nm(seg_pts, box):
    """Longest continuous distance (nm) a track stays on a runway's final.

    A segment extends the run when both endpoints are inside the box and its
    course aligns with the landing heading within RUNWAY_MATCH_TOLERANCE_DEG;
    any non-qualifying segment resets the run to zero. Returns the longest such
    run seen. This measures how far the aircraft was *established* on the
    centerline, which distinguishes a real final from a pattern turn that only
    clips the box for one segment.
    """
    longest = 0.0
    run = 0.0
    for i in range(1, len(seg_pts)):
        lat0, lon0 = seg_pts[i - 1]
        lat1, lon1 = seg_pts[i]
        # A repeated position (common when ADS-B re-sends the same fix) has no
        # defined bearing; skip it without resetting the run so a stationary
        # blip mid-final doesn't break an otherwise continuous established run.
        if (lat0, lon0) == (lat1, lon1):
            continue
        course = _bearing_deg(lat0, lon0, lat1, lon1)
        if (_point_in_approach_box(lat0, lon0, box)
                and _point_in_approach_box(lat1, lon1, box)
                and _circular_diff(course, box["heading"])
                <= RUNWAY_MATCH_TOLERANCE_DEG):
            run += _fast_distance_nm(lat0, lon0, lat1, lon1)
            longest = max(longest, run)
        else:
            run = 0.0
    return longest


def _approach_runway(seg_pts, boxes):
    """Return the runway ident a track approached, or None.

    seg_pts: [(lat, lon), ...] ordered by time — the low-altitude in-ring
    points of one track. For each runway we measure the longest continuous run
    the track stays established on that runway's final (on the extended
    centerline and aligned; see _longest_final_run_nm). The track votes for the
    runway with the longest such run, provided it reaches MIN_FINAL_RUN_NM.
    Downwind and base legs sit off the centerline or cross it at an angle, so
    they don't accumulate a run; a tight-pattern turn that momentarily clips a
    box produces only a tiny run and loses to the runway actually flown. A
    track that never establishes on any final casts no vote.
    """
    # best_run starts at the MIN_FINAL_RUN_NM threshold so a runway must clear
    # it to win. On an exact tie (`>=`), the later box in iteration order wins;
    # ties between two real finals are vanishingly unlikely (runs are float
    # nm), so the arbitrary-but-deterministic choice is acceptable.
    best_ident = None
    best_run = MIN_FINAL_RUN_NM
    for box in boxes:
        run = _longest_final_run_nm(seg_pts, box)
        if run >= best_run:
            best_run = run
            best_ident = box["ident"]
    return best_ident


def _split_on_time_gaps(timed_pts):
    """Yield sub-lists of timed_pts split at gaps > APPROACH_SPLIT_GAP_S.

    Separates distinct visits: an aircraft that leaves and comes back (or
    disappears from coverage between pattern circuits) starts a new approach.
    """
    visit = []
    prev_ts = None
    for pt in timed_pts:
        ts = pt[0]
        if prev_ts is not None and ts - prev_ts > APPROACH_SPLIT_GAP_S:
            yield visit
            visit = []
        visit.append(pt)
        prev_ts = ts
    if visit:
        yield visit


def _split_at_field_crossings(timed_pts, field_lat, field_lon):
    """Yield sub-lists of timed_pts split after each field overpass.

    A continuously-tracked touch-and-go crosses the field, climbs out, flies a
    pattern, and approaches again with no time gap. We cut after each overpass
    so each approach votes separately. Hysteresis (CROSS_NEAR_NM to arm the
    cut, CROSS_FAR_NM to re-arm) keeps GPS jitter near the threshold from
    splitting a single approach into pieces.

    Deliberately conservative: a very tight circuit whose climb-out never
    reaches CROSS_FAR_NM won't split, so two such approaches count as one. This
    under-counts rather than risk over-splitting a single approach — the safe
    failure for a usage *distribution*, where the ratio matters more than the
    absolute count.
    """
    sub = []
    armed = True   # True until we pass over the field; then wait to head back out
    for ts, lat, lon in timed_pts:
        sub.append((ts, lat, lon))
        dist = _fast_distance_nm(field_lat, field_lon, lat, lon)
        if armed and dist < CROSS_NEAR_NM:
            armed = False           # overpass seen; don't cut yet (still near field)
        elif not armed and dist > CROSS_FAR_NM:
            yield sub                # heading back out after an overpass -> cut
            sub = []
            armed = True
    if sub:
        yield sub


def runway_votes_for_track(timed_pts, boxes):
    """Runway votes for one aircraft's low, in-ring points (one per approach).

    timed_pts: [(now, lat, lon), ...] ordered by time — the low-altitude
    in-ring points of a single hex. The points are split into individual
    approaches two ways: on time gaps between visits (_split_on_time_gaps) and,
    within a continuous visit, at each field overpass (_split_at_field_crossings,
    to catch touch-and-goes). Each resulting sub-track votes once via
    _approach_runway. Returns the list of non-None runway idents (may be empty,
    or hold several for pattern work).

    The field center for crossing detection is the centroid of the runway
    thresholds — within ~150 ft of the airport reference point, far finer than
    the crossing radius, so no separate airport-location argument is needed.
    """
    if not boxes:
        return []
    field_lat = sum(b["lat"] for b in boxes) / len(boxes)
    field_lon = sum(b["lon"] for b in boxes) / len(boxes)

    votes = []
    for visit in _split_on_time_gaps(timed_pts):
        for sub in _split_at_field_crossings(visit, field_lat, field_lon):
            ident = _approach_runway([(la, lo) for _, la, lo in sub], boxes)
            if ident is not None:
                votes.append(ident)
    return votes
