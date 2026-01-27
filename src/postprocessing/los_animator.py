"""Generate animated Folium maps for LOS (Loss of Separation) events.

This module creates interactive HTML maps with time sliders showing
aircraft positions before, during, and after proximity events.
"""

import datetime
import logging
from typing import List, Optional, Tuple

import folium
from folium.plugins import TimestampedGeoJson

logger = logging.getLogger(__name__)


class LOSAnimator:
    """Generate animated Folium maps for LOS events."""

    # Colors for the two aircraft
    AIRCRAFT1_COLOR = "blue"
    AIRCRAFT2_COLOR = "red"

    def __init__(self, resampler):
        """Initialize the animator.

        Args:
            resampler: Resampler instance with locations_by_flight_id data
        """
        self.resampler = resampler

    def _find_flight_id(self, tail: str, event_time: float) -> Optional[str]:
        """Find the resampler flight_id that matches a tail number near event time.

        The resampler uses flight_ids like "N12345_1" while LOS stores just "N12345".
        This finds the matching suffixed ID by looking for data near the event time.

        Args:
            tail: Tail number (e.g., "N12345")
            event_time: Unix timestamp of the event

        Returns:
            Matching flight_id or None if not found
        """
        # Search locations_by_time near event_time to find matching flight_ids
        candidates = set()
        search_window = 120  # seconds to search around event

        for t in range(int(event_time) - search_window, int(event_time) + search_window):
            if t not in self.resampler.locations_by_time:
                continue
            for loc in self.resampler.locations_by_time[t]:
                if loc.flight and (loc.flight.startswith(tail + "_") or loc.flight == tail):
                    candidates.add(loc.flight)

        if not candidates:
            logger.warning("No flight_ids found matching tail %s near time %d", tail, int(event_time))
            return None

        if len(candidates) == 1:
            return list(candidates)[0]

        # Multiple candidates - find the one with data closest to event_time
        best_fid = None
        best_dist = float('inf')
        for t in range(int(event_time) - search_window, int(event_time) + search_window):
            if t not in self.resampler.locations_by_time:
                continue
            for loc in self.resampler.locations_by_time[t]:
                if loc.flight in candidates:
                    dist = abs(loc.now - event_time)
                    if dist < best_dist:
                        best_dist = dist
                        best_fid = loc.flight

        logger.debug("Multiple candidates for %s, chose %s (dist=%.0fs from event)",
                    tail, best_fid, best_dist)
        return best_fid

    def _get_positions_in_window(self, flight_id: str, start_time: float,
                                  end_time: float) -> List:
        """Extract positions for a flight within a time window.

        Uses locations_by_time which contains both original and interpolated
        (resampled) positions for per-second granularity.

        Args:
            flight_id: Flight identifier (e.g., "N12345_1")
            start_time: Start of window (Unix timestamp)
            end_time: End of window (Unix timestamp)

        Returns:
            List of Location objects within the time window
        """
        positions = []

        # Use locations_by_time for resampled (per-second) data
        for t in range(int(start_time), int(end_time) + 1):
            if t not in self.resampler.locations_by_time:
                continue
            for loc in self.resampler.locations_by_time[t]:
                # Match by flight field (which contains the suffixed ID like N12345_1)
                if loc.flight == flight_id:
                    positions.append(loc)

        if not positions:
            logger.warning("Flight %s not found in resampler time data", flight_id)

        return sorted(positions, key=lambda x: x.now)

    def _build_features(self, positions: List, color: str,
                        tail_number: str = "") -> List[dict]:
        """Build GeoJSON features for a list of positions.

        Creates circle markers with embedded label data. Labels are rendered
        via custom JavaScript injected into the map.

        Args:
            positions: List of Location objects
            color: Color for markers (e.g., "blue", "red")
            tail_number: Tail number for labeling

        Returns:
            List of GeoJSON Feature dicts
        """
        features = []

        for loc in positions:
            # Format timestamp as ISO string for TimestampedGeoJson
            time_str = datetime.datetime.utcfromtimestamp(loc.now).isoformat() + "Z"

            # Build label text
            tail = tail_number or loc.tail or loc.flight or "?"
            alt = loc.alt_baro if loc.alt_baro else 0

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [loc.lon, loc.lat]
                },
                "properties": {
                    "time": time_str,
                    "popup": f"<b>{tail}</b><br>Alt: {alt} ft",
                    "icon": "circle",
                    "iconstyle": {
                        "color": color,
                        "fillColor": color,
                        "fillOpacity": 0.9,
                        "radius": 5
                    },
                    "tail": tail,
                    "alt": alt
                }
            }
            features.append(feature)

        return features

    def _build_trail_features(self, positions: List, color: str) -> List[dict]:
        """Build GeoJSON LineString features showing the flight path.

        Args:
            positions: List of Location objects (sorted by time)
            color: Color for the trail line

        Returns:
            List of GeoJSON Feature dicts for the trail
        """
        if len(positions) < 2:
            return []

        # Build cumulative line segments - each timestamp shows path up to that point
        features = []

        for i in range(1, len(positions)):
            # Coordinates up to this point
            coords = [[loc.lon, loc.lat] for loc in positions[:i+1]]
            time_str = datetime.datetime.utcfromtimestamp(
                positions[i].now).isoformat() + "Z"

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": coords
                },
                "properties": {
                    "time": time_str,
                    "style": {
                        "color": color,
                        "weight": 3,
                        "opacity": 0.6
                    }
                }
            }
            features.append(feature)

        return features

    def animate_los(self, flight1_id: str, flight2_id: str,
                    event_time: float, window_before: int = 240,
                    window_after: int = 60,
                    output_file: str = "los_animation.html",
                    map_type: str = "sectional") -> Optional[str]:
        """Create animated map showing two aircraft around an LOS event.

        Args:
            flight1_id: First aircraft flight_id (e.g., "N12345_1")
            flight2_id: Second aircraft flight_id
            event_time: Unix timestamp of closest approach
            window_before: Seconds before event to show (default 120)
            window_after: Seconds after event to show (default 60)
            output_file: Output HTML file path
            map_type: Base map type ("sectional" or "satellite")

        Returns:
            Output file path if successful, None otherwise
        """
        start_time = event_time - window_before
        end_time = event_time + window_after

        logger.info("Generating LOS animation for %s vs %s",
                    flight1_id, flight2_id)
        logger.info("Time window: %s to %s",
                    datetime.datetime.utcfromtimestamp(start_time),
                    datetime.datetime.utcfromtimestamp(end_time))

        # Get positions for both aircraft
        positions1 = self._get_positions_in_window(flight1_id, start_time, end_time)
        positions2 = self._get_positions_in_window(flight2_id, start_time, end_time)

        if not positions1:
            logger.error("No positions found for flight %s", flight1_id)
            return None
        if not positions2:
            logger.error("No positions found for flight %s", flight2_id)
            return None

        logger.info("Found %d positions for %s, %d for %s",
                    len(positions1), flight1_id, len(positions2), flight2_id)

        # Calculate map center from all positions
        all_lats = [p.lat for p in positions1 + positions2]
        all_lons = [p.lon for p in positions1 + positions2]
        center_lat = sum(all_lats) / len(all_lats)
        center_lon = sum(all_lons) / len(all_lons)

        # Create base map
        m = folium.Map(location=[center_lat, center_lon], zoom_start=13, tiles=None)

        # Add base layer
        # Use max_native_zoom to allow zooming past tile server limits (tiles get upscaled)
        if map_type == "satellite":
            folium.TileLayer(
                tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
                attr="Esri World Imagery",
                opacity=0.8,
                max_native_zoom=18,
                max_zoom=22
            ).add_to(m)
        else:
            # VFR Sectional
            folium.TileLayer(
                tiles="https://tiles.arcgis.com/tiles/ssFJjBXIUyZDrSYZ/arcgis/rest/services/VFR_Sectional/MapServer/tile/{z}/{y}/{x}",
                attr="Esri VFR Sectional",
                opacity=0.8,
                max_native_zoom=12,
                max_zoom=22
            ).add_to(m)

        # Get tail numbers for labels
        tail1 = positions1[0].tail if positions1 else flight1_id
        tail2 = positions2[0].tail if positions2 else flight2_id

        # Add static trail lines (always visible)
        trail1_coords = [[p.lat, p.lon] for p in positions1]
        trail2_coords = [[p.lat, p.lon] for p in positions2]

        folium.PolyLine(
            trail1_coords,
            color=self.AIRCRAFT1_COLOR,
            weight=2,
            opacity=0.7
        ).add_to(m)

        folium.PolyLine(
            trail2_coords,
            color=self.AIRCRAFT2_COLOR,
            weight=2,
            opacity=0.7
        ).add_to(m)

        # Build animated point features with altitude labels
        features = []
        features.extend(self._build_features(positions1, self.AIRCRAFT1_COLOR, tail1))
        features.extend(self._build_features(positions2, self.AIRCRAFT2_COLOR, tail2))

        # Add TimestampedGeoJson layer for animated dots
        TimestampedGeoJson(
            {"type": "FeatureCollection", "features": features},
            period="PT1S",           # 1-second intervals
            duration="PT1S",         # Each point visible for 1 second
            transition_time=200,     # 200ms animation between frames
            auto_play=False,         # Start paused
            add_last_point=False,    # Don't add default markers
            loop=False,
            max_speed=10,
            loop_button=True,
            time_slider_drag_update=True
        ).add_to(m)

        # Add altitude labels along the track every 20 seconds
        label_interval = 20  # seconds
        for positions, tail, color in [
            (positions1, tail1, self.AIRCRAFT1_COLOR),
            (positions2, tail2, self.AIRCRAFT2_COLOR)
        ]:
            if not positions:
                continue
            start_time_pos = positions[0].now
            for i, pos in enumerate(positions):
                seconds_elapsed = pos.now - start_time_pos
                # Label at start (with tail), then every 20 seconds (altitude only)
                if i == 0:
                    label_text = f"{tail} {pos.alt_baro}'"
                elif seconds_elapsed > 0 and int(seconds_elapsed) % label_interval == 0:
                    # Only add if this is the first position at this interval
                    prev_elapsed = positions[i-1].now - start_time_pos if i > 0 else -1
                    if int(prev_elapsed) // label_interval < int(seconds_elapsed) // label_interval:
                        label_text = f"{pos.alt_baro}'"
                    else:
                        continue
                else:
                    continue

                folium.Marker(
                    location=[pos.lat, pos.lon],
                    icon=folium.DivIcon(
                        html=f'<div style="font-size:11px;color:{color};font-weight:bold;white-space:nowrap;text-shadow: 1px 1px 1px white;">{label_text}</div>',
                        icon_anchor=(0, 0)
                    )
                ).add_to(m)

        # Add legend with tail numbers
        legend_html = f'''
        <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000;
                    background-color: white; padding: 10px; border-radius: 5px;
                    border: 2px solid gray; font-size: 14px;">
            <b>LOS Event</b><br>
            <span style="color: {self.AIRCRAFT1_COLOR};">●</span> {tail1}<br>
            <span style="color: {self.AIRCRAFT2_COLOR};">●</span> {tail2}<br>
            <small>CPA: {datetime.datetime.utcfromtimestamp(event_time).strftime("%H:%M:%S")} UTC</small><br>
            <small>Click dots for altitude</small>
        </div>
        '''
        m.get_root().html.add_child(folium.Element(legend_html))

        # Mark CPA location with a simple circle (not a marker icon)
        cpa_pos1 = min(positions1, key=lambda p: abs(p.now - event_time))
        cpa_pos2 = min(positions2, key=lambda p: abs(p.now - event_time))
        cpa_lat = (cpa_pos1.lat + cpa_pos2.lat) / 2
        cpa_lon = (cpa_pos1.lon + cpa_pos2.lon) / 2

        folium.CircleMarker(
            location=[cpa_lat, cpa_lon],
            radius=12,
            color="orange",
            fill=False,
            weight=3,
            popup=f"CPA: {datetime.datetime.utcfromtimestamp(event_time).strftime('%H:%M:%S')} UTC"
        ).add_to(m)

        # Save the map
        m.save(output_file)
        logger.info("Animation saved to %s", output_file)

        return output_file

    def animate_from_los_object(self, los, window_before: int = 120,
                                 window_after: int = 60,
                                 output_file: str = None) -> Optional[str]:
        """Create animation from an LOS object.

        Args:
            los: LOS object from los.py with flight1, flight2, create_time
            window_before: Seconds before event to show
            window_after: Seconds after event to show
            output_file: Output file path (auto-generated if None)

        Returns:
            Output file path if successful, None otherwise
        """
        tail1 = los.flight1.flight_id.strip()
        tail2 = los.flight2.flight_id.strip()

        # Use create_time as the event time (when LOS was first detected)
        event_time = los.create_time

        # Find the resampler flight_ids (with _N suffix) that match these tails
        flight1_id = self._find_flight_id(tail1, event_time)
        flight2_id = self._find_flight_id(tail2, event_time)

        if not flight1_id:
            print(f"  Skipping: Could not find resampler data for {tail1}")
            return None
        if not flight2_id:
            print(f"  Skipping: Could not find resampler data for {tail2}")
            return None

        if output_file is None:
            timestamp = datetime.datetime.utcfromtimestamp(event_time)
            output_file = f"los_{tail1}_{tail2}_{timestamp.strftime('%Y%m%d_%H%M%S')}.html"

        result = self.animate_los(
            flight1_id, flight2_id, event_time,
            window_before, window_after, output_file
        )

        if result:
            print(f"  Created: {output_file}")

        return result
