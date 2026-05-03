#!/usr/bin/env python3
"""
LOS Detector: Wraps adsb_actions to run LOS analysis on a grid cell shard
and collect results as structured records (Parquet + CSV).

This module reuses the existing LOS detection pipeline (AdsbActions, Resampler,
LOS object, quality scoring) and redirects output from CSV log lines to an
in-memory list of dicts that can be written to Parquet/CSV.

Usage (as a library):
    from hotspots.los_detector import LOSDetector
    detector = LOSDetector(animate=True, animation_dir="data/v2/animations/20260101_37_-122")
    detector.run(shard_gz="data/v2/grid/20260101_37_-122.gz")
    detector.write_parquet("data/v2/events/20260101_37_-122.parquet")
    detector.write_csv("data/v2/events/20260101_37_-122.csv")
"""

import datetime
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional

# Allow running from project root or src/hotspots/
_ROOT = Path(__file__).resolve().parents[2]
for _p in [str(_ROOT / "src"), str(_ROOT)]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

import math

import pandas as pd

from adsb_actions.adsbactions import AdsbActions
from adsb_actions.adsb_logger import Logger
from lib import replay

logger = logging.getLogger(__name__)
LOGGER = Logger()

# Several adsb_actions modules set their logger levels unconditionally (DEBUG/INFO).
# Suppress the whole package to WARNING in batch context to keep output clean.
# Individual child loggers must be listed because they override the parent level directly.
for _noisy in ("adsb_actions", "adsb_actions.adsbactions", "adsb_actions.resampler",
               "adsb_actions.rules", "adsb_actions.rules_optimizations",
               "adsb_actions.flights", "adsb_actions.flight",
               "adsb_actions.location", "adsb_actions.bboxes", "adsb_actions.los"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

# LOS thresholds (must match the YAML config)
PROX_ALT_FT = 400
PROX_LAT_NM = 0.3

# MSL altitude bands in feet. Upper bound matches the resampler's
# DEFAULT_MAX_ALTITUDE (10,000 ft) — events above that are dropped upstream,
# so higher bands would always be empty.
ALT_BANDS = [(0, 3000, "0k-3k"), (3000, 6000, "3k-6k"), (6000, 10001, "6k-10k")]

# Spatial altitude filter parameters:
# Events within AIRPORT_EXCLUSION_RADIUS_NM of an airport are required to be
# at least AIRPORT_AGL_MIN_FT above that airport's field elevation, to exclude
# ground-ops taxi/gate proximity events.
# Events further from any airport use OPEN_TERRAIN_MIN_ALT_FT as a floor.
AIRPORT_EXCLUSION_RADIUS_NM = 5.0
AIRPORT_AGL_MIN_FT = 200       # min height above field elevation near airports
OPEN_TERRAIN_MIN_ALT_FT = 50   # floor away from any airport (catches terrain extremes)


def alt_band_label(alt_ft) -> str:
    """Return MSL band label for an altitude in feet."""
    try:
        alt = float(alt_ft)
    except (TypeError, ValueError):
        return "unknown"
    for lo, hi, label in ALT_BANDS:
        if lo <= alt < hi:
            return label
    return "10k+"


def _dist_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Fast equirectangular distance in nautical miles."""
    dlat = lat2 - lat1
    dlon = (lon2 - lon1) * math.cos(math.radians((lat1 + lat2) / 2))
    return math.sqrt(dlat * dlat + dlon * dlon) * 60.0


def _load_airports_in_cell(lat_min: int, lat_max: int,
                            lon_min: int, lon_max: int) -> list:
    """
    Return list of (lat, lon, elevation_ft) for all airports within or
    near the cell (expanded by AIRPORT_EXCLUSION_RADIUS_NM).

    Uses the OurAirports CSV via generate_airport_config's cached index.
    """
    # Expand bounds by ~0.1° (≈6nm) to catch airports just outside the cell
    # whose exclusion radius overlaps into it.
    margin_deg = AIRPORT_EXCLUSION_RADIUS_NM / 60.0
    lat_lo = lat_min - margin_deg
    lat_hi = lat_max + margin_deg
    lon_lo = lon_min - margin_deg
    lon_hi = lon_max + margin_deg

    try:
        from tools.generate_airport_config import _build_airport_index, _airport_index
        import tools.generate_airport_config as gac
        if gac._airport_index is None:
            gac._build_airport_index()
        index = gac._airport_index
    except Exception as e:
        logger.warning("Could not load airport index: %s — skipping spatial alt filter", e)
        return []

    airports = []
    seen = set()
    for row in index.values():
        try:
            lat = float(row.get('latitude_deg') or 0)
            lon = float(row.get('longitude_deg') or 0)
            elev = float(row.get('elevation_ft') or 0)
        except (ValueError, TypeError):
            continue
        if not (lat_lo <= lat <= lat_hi and lon_lo <= lon <= lon_hi):
            continue
        key = (round(lat, 4), round(lon, 4))
        if key in seen:
            continue
        seen.add(key)
        airports.append((lat, lon, elev))

    return airports


def _make_yaml_content() -> str:
    """Generate minimal YAML config for grid cell proximity analysis.

    No min_alt here — altitude filtering is done post-detection in
    LOSDetector._apply_altitude_filter() using per-airport spatial logic.
    """
    return (
        "rules:\n"
        "    prox_analysis:\n"
        "        conditions:\n"
        f"            proximity: [{PROX_ALT_FT}, {PROX_LAT_NM}]\n"
        "        actions:\n"
        "            callback: los_update_cb\n"
    )


class LOSDetector:
    """
    Runs LOS analysis on a grid cell shard.

    Instead of printing CSV lines to stdout (v1 behavior), this class
    intercepts events at finalization time and stores them as dicts
    for Parquet/CSV output.
    """

    def __init__(self, animate: bool = False, animation_dir: Optional[str] = None):
        self.animate = animate
        self.animation_dir = animation_dir
        self.events: List[dict] = []
        self._resampling_started = False
        self._airports: Optional[list] = None  # loaded lazily per cell

    def _los_cb(self, flight1, flight2):
        """LOS callback: only fires during resampling phase (same gate as v1)."""
        if self._resampling_started:
            from applications.airport_monitor.los import process_los_launch
            process_los_launch(flight1, flight2, do_threading=False)

    def _los_gc_interceptor(self, ts):
        """
        Drop-in replacement for los_gc() that intercepts finalized events
        and appends them to self.events instead of (only) writing CSV log lines.

        Calls the original los_gc for its side-effects (quality scoring,
        animation generation, database update stub), then harvests whatever
        it finalized into self.events.
        """
        from applications.airport_monitor.los import los_gc, LOS

        # Snapshot current events before GC so we can detect newly finalized ones
        before_keys = set(LOS.current_los_events.keys())

        los_gc(ts)  # runs quality scoring, animation, moves to finalized_los_events

        # Newly finalized = present in finalized but moved out of current
        after_keys = set(LOS.current_los_events.keys())
        newly_finalized_keys = before_keys - after_keys

        for key in newly_finalized_keys:
            los = LOS.finalized_los_events.get(key)
            if los is None:
                continue
            self._harvest_event(los)

    def _extract_track(self, flight_id: str, cpa_time: float) -> str:
        """
        Extract resampled track data for one flight around the CPA time window.

        Queries LOS.resampler.locations_by_time (includes interpolated points)
        for the window [cpa_time - 120s, cpa_time + 60s], filtering to locations
        whose .flight matches flight_id.

        Returns a JSON array string of [timestamp, lat, lon, alt_ft] tuples,
        or "" if no data is available.
        """
        from applications.airport_monitor.los import LOS
        import json as _json

        resampler = LOS.resampler
        if resampler is None:
            return ""

        t_start = int(cpa_time) - 120
        t_end = int(cpa_time) + 60
        points = []
        lbt = resampler.locations_by_time
        for t in range(t_start, t_end + 1):
            for loc in lbt.get(t, []):
                if loc.flight == flight_id:
                    points.append([t, loc.lat, loc.lon, loc.alt_baro, 1 if loc.resampled else 0])
        return _json.dumps(points)

    def _harvest_event(self, los):
        """Convert a finalized LOS object into a dict and append to self.events."""
        from adsb_actions.location import Location

        meanloc = Location.meanloc(los.first_loc_1, los.first_loc_2)
        alt_ft = meanloc.alt_baro if meanloc.alt_baro is not None else 0

        from applications.airport_monitor.los import calculate_event_quality
        quality, quality_explanation = calculate_event_quality(los, los.flight1, los.flight2)

        cpa_dt = datetime.datetime.utcfromtimestamp(los.cpa_time)

        # Resampler flight IDs (e.g. "N12345_1") are stored on first_loc_1/2.flight
        fid1 = los.first_loc_1.flight if los.first_loc_1 else los.flight1.flight_id.strip()
        fid2 = los.first_loc_2.flight if los.first_loc_2 else los.flight2.flight_id.strip()

        event = {
            "timestamp": los.cpa_time,
            "datetime_utc": cpa_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "lat": meanloc.lat,
            "lon": meanloc.lon,
            "alt_ft": alt_ft,
            "alt_band": alt_band_label(alt_ft),
            "flight1": los.flight1.flight_id.strip(),
            "flight2": los.flight2.flight_id.strip(),
            "lateral_nm": los.min_latdist,
            "alt_sep_ft": los.min_altdist,
            "duration_s": los.last_time - los.create_time,
            "quality": quality,
            "quality_explanation": quality_explanation,
            "animation_path": "",  # filled below if available
            "track1": self._extract_track(fid1, los.cpa_time),
            "track2": self._extract_track(fid2, los.cpa_time),
        }

        # Pick up animation path from LOS.finalized_los_events side-effects
        # los_gc() calls _generate_animation internally and its result is logged
        # but not stored on the LOS object. We detect it via animation_output_dir.
        if self.animate and self.animation_dir:
            tail1 = los.flight1.flight_id.strip()
            tail2 = los.flight2.flight_id.strip()
            ts_str = cpa_dt.strftime("%Y%m%d_%H%M%S")
            candidate = os.path.join(self.animation_dir,
                                     f"los_{tail1}_{tail2}_{ts_str}.html")
            if os.path.exists(candidate):
                event["animation_path"] = candidate

        self.events.append(event)

    def _is_airborne(self, event: dict) -> bool:
        """
        Return True if this event's altitude is above the ground-ops threshold
        for its location.

        Near an airport: require alt > field_elev + AIRPORT_AGL_MIN_FT.
        Away from all airports: require alt > OPEN_TERRAIN_MIN_ALT_FT.

        This handles varying terrain across the cell without needing a DEM:
        airports are the only places where ground-ops noise occurs. Events in
        open mountainous terrain won't be near any airport, so the low
        OPEN_TERRAIN_MIN_ALT_FT floor applies there instead.
        """
        try:
            alt = float(event["alt_ft"])
        except (TypeError, ValueError, KeyError):
            return False

        lat = event.get("lat")
        lon = event.get("lon")
        if lat is None or lon is None:
            return alt > OPEN_TERRAIN_MIN_ALT_FT

        # Check proximity to known airports
        if self._airports is not None:
            for ap_lat, ap_lon, ap_elev in self._airports:
                dist = _dist_nm(lat, lon, ap_lat, ap_lon)
                if dist <= AIRPORT_EXCLUSION_RADIUS_NM:
                    # Near this airport: require AIRPORT_AGL_MIN_FT above its field
                    return alt > ap_elev + AIRPORT_AGL_MIN_FT

        # Not near any airport: apply open-terrain floor
        return alt > OPEN_TERRAIN_MIN_ALT_FT

    def run(self, shard_gz: str,
            lat_min: Optional[int] = None, lat_max: Optional[int] = None,
            lon_min: Optional[int] = None, lon_max: Optional[int] = None) -> int:
        """
        Run LOS analysis on a grid cell shard.

        Args:
            shard_gz: Path to input gzip JSONL shard file.
            lat_min, lat_max, lon_min, lon_max: Cell bounds (integers).
                If provided, loads airports within the cell for spatial altitude
                filtering. If omitted, only OPEN_TERRAIN_MIN_ALT_FT is applied.

        Returns:
            Number of LOS events after altitude filtering.
        """
        from applications.airport_monitor.los import LOS

        # Load airports for this cell so _is_airborne() can do spatial checks
        if lat_min is not None:
            self._airports = _load_airports_in_cell(lat_min, lat_max, lon_min, lon_max)
            logger.debug("Loaded %d airports for cell %d_%d", len(self._airports), lat_min, lon_min)
        else:
            self._airports = []

        # Reset LOS class-level state between runs
        LOS.current_los_events = {}
        LOS.finalized_los_events = {}
        LOS.quit = False

        # Write YAML — use a per-process temp file to avoid races with parallel workers
        import tempfile
        yaml_content = _make_yaml_content()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tf:
            tf.write(yaml_content)
            yaml_path = tf.name

        try:
            adsb_actions = AdsbActions(yaml_file=yaml_path, pedantic=True,
                                       resample=True, use_optimizations=True)

            # Re-apply after construction: adsb_actions modules set logger.level at
            # import time, so all imports triggered by AdsbActions.__init__ have now
            # run and we can override them cleanly.
            for _noisy in ("adsb_actions", "adsb_actions.adsbactions",
                           "adsb_actions.resampler", "adsb_actions.rules",
                           "adsb_actions.rules_optimizations", "adsb_actions.flights",
                           "adsb_actions.flight", "adsb_actions.location",
                           "adsb_actions.bboxes", "adsb_actions.los"):
                logging.getLogger(_noisy).setLevel(logging.WARNING)

            adsb_actions.register_callback("los_update_cb", self._los_cb)

            # Set up animation if requested
            if self.animate and self.animation_dir:
                from postprocessing.los_animator import LOSAnimator
                os.makedirs(self.animation_dir, exist_ok=True)
                LOS.animation_output_dir = self.animation_dir
                LOS.animator = LOSAnimator(adsb_actions.resampler)
            else:
                LOS.animator = None

            LOS.resampler = adsb_actions.resampler

            # Streaming pass
            iterator = replay.yield_from_sorted_file(shard_gz)
            adsb_actions.loop(iterator_data=iterator)
            adsb_actions.rules.close_emit_files()

            # Resampling pass (primary LOS detection)
            self._resampling_started = True
            adsb_actions.do_resampled_prox_checks(self._los_gc_interceptor,
                                                   label=os.path.basename(shard_gz))

            # Final GC pass to catch any remaining open events
            # Use the latest timestamp + LOS_GC_TIME to force finalization
            if LOS.current_los_events:
                max_ts = max(v.last_time for v in LOS.current_los_events.values())
                self._los_gc_interceptor(max_ts + LOS.LOS_GC_TIME + 1)

        finally:
            self._resampling_started = False
            LOS.animator = None
            try:
                os.remove(yaml_path)
            except OSError:
                pass

        # Apply spatial altitude filter: remove on-ground / taxi events
        before = len(self.events)
        self.events = [e for e in self.events if self._is_airborne(e)]
        filtered = before - len(self.events)
        if filtered:
            logger.debug("Altitude filter removed %d ground-ops events (%d remain)",
                         filtered, len(self.events))

        return len(self.events)

    def write_parquet(self, path: str) -> None:
        """Write collected events to a Parquet file. Skips write if no events;
        caller should use write_empty_sentinel() for skip_existing support."""
        if not self.events:
            return
        p = Path(path)
        os.makedirs(p.parent, exist_ok=True)
        tmp = p.with_suffix(".parquet.tmp")
        try:
            pd.DataFrame(self.events).to_parquet(str(tmp), index=False)
            tmp.rename(p)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

    def write_csv(self, path: str) -> None:
        """Write collected events to a clean CSV file (no postprocessing prefix).
        Track data is omitted — it's large JSON and only needed in the parquet.
        Skips write if no events."""
        if not self.events:
            return
        p = Path(path)
        os.makedirs(p.parent, exist_ok=True)
        tmp = p.with_suffix(".csv.tmp")
        try:
            pd.DataFrame(self.events).drop(columns=["track1", "track2"], errors="ignore").to_csv(str(tmp), index=False)
            tmp.rename(p)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

    def write_empty_sentinel(self, parquet_path: str) -> None:
        """Write a zero-byte sentinel so skip_existing can identify processed empty cells."""
        p = Path(parquet_path).with_suffix(".empty")
        os.makedirs(p.parent, exist_ok=True)
        p.touch()
