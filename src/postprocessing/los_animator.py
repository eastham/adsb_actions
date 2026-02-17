"""Generate animated Folium maps for LOS (Loss of Separation) events.

This module creates interactive HTML maps with time sliders showing
aircraft positions before, during, and after proximity events.
"""

import datetime
import logging

from typing import List, Optional

import folium
from folium.plugins import TimestampedGeoJson

logger = logging.getLogger(__name__)


class LOSAnimator:
    """Generate animated Folium maps for LOS events."""

    # Colors for the two aircraft
    AIRCRAFT1_COLOR = "blue"
    AIRCRAFT2_COLOR = "red"
    GAP_COLOR = "gray"         # color for segments spanning a data gap
    GAP_THRESHOLD_S = 10       # seconds; gaps larger than this are highlighted

    def __init__(self, resampler):
        """Initialize the animator.

        Args:
            resampler: Resampler instance with locations_by_flight_id data
        """
        self.resampler = resampler

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
                if loc.flight == flight_id:
                    positions.append(loc)

        if not positions:
            logger.warning("Flight %s not found in resampler time data", flight_id)

        return sorted(positions, key=lambda x: x.now)

    def _build_features(self, positions: List, color: str) -> List[dict]:
        """Build GeoJSON Point features for TimestampedGeoJson animation."""
        features = []

        for loc in positions:
            time_str = datetime.datetime.utcfromtimestamp(loc.now).isoformat() + "Z"
            alt = loc.alt_baro if loc.alt_baro else 0

            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [round(loc.lon, 6), round(loc.lat, 6)]
                },
                "properties": {
                    "time": time_str,
                    "popup": f"{alt}ft",
                    "icon": "circle",
                    "iconstyle": {
                        "color": color,
                        "fillColor": color,
                        "fillOpacity": 0.9,
                        "radius": 5
                    }
                }
            }
            features.append(feature)

        return features

    def _add_trail_polylines(self, m, positions: List, color: str,
                               weight: int = 2, opacity: float = 0.7,
                               los_start: float = None, los_end: float = None):
        """Add trail polylines to a map with visual distinction for:
        - Interpolated gaps (grey dashed)
        - Active LOS interval (bold weight)
        - Context before/after LOS (normal weight)

        Uses Location.resampled flag to detect stretches interpolated
        across a data gap (> GAP_THRESHOLD_S consecutive resampled points).
        """
        if len(positions) < 2:
            return

        GAP_WEIGHT_MULT = 1
        LOS_WEIGHT_MULT = 4

        # Determine which positions are in a significant interpolated gap.
        in_gap = [False] * len(positions)
        i = 0
        while i < len(positions):
            if positions[i].resampled:
                run_start = i
                while i < len(positions) and positions[i].resampled:
                    i += 1
                run_duration = positions[i - 1].now - positions[run_start].now
                if run_duration > self.GAP_THRESHOLD_S:
                    for j in range(run_start, i):
                        in_gap[j] = True
            else:
                i += 1

        def _style_key(idx):
            """Return a tuple representing the visual style for position idx."""
            gap = in_gap[idx]
            in_los = (los_start is not None and los_end is not None and
                      los_start <= positions[idx].now <= los_end)
            return (gap, in_los)

        def _make_style(key):
            gap, in_los = key
            if gap:
                return dict(color=self.GAP_COLOR, weight=weight * GAP_WEIGHT_MULT,
                            opacity=opacity, dash_array="6 4")
            if in_los:
                return dict(color="magenta", weight=weight * LOS_WEIGHT_MULT,
                            opacity=opacity)
            return dict(color=color, weight=weight, opacity=opacity)

        # Walk positions, grouping consecutive same-style segments
        seg_coords = [[positions[0].lat, positions[0].lon]]
        prev_key = _style_key(0)

        for i in range(1, len(positions)):
            cur_key = _style_key(i)
            if cur_key != prev_key:
                if len(seg_coords) >= 2:
                    folium.PolyLine(seg_coords, **_make_style(prev_key)).add_to(m)
                # Overlap one point for continuity
                seg_coords = [seg_coords[-1]]
                prev_key = cur_key
            seg_coords.append([positions[i].lat, positions[i].lon])

        # Emit final segment
        if len(seg_coords) >= 2:
            folium.PolyLine(seg_coords, **_make_style(prev_key)).add_to(m)

    def animate_los(self, flight1_id: str, flight2_id: str,
                    event_time: float, window_before: int = 240,
                    window_after: int = 60,
                    output_file: str = "los_animation.html",
                    map_type: str = "sectional",
                    los_start: float = None,
                    los_end: float = None) -> Optional[str]:
        """Create animated map showing two aircraft around an LOS event.

        Args:
            flight1_id: First aircraft flight_id (e.g., "N12345_1")
            flight2_id: Second aircraft flight_id
            event_time: Unix timestamp of closest approach
            window_before: Seconds before event to show (default 120)
            window_after: Seconds after event to show (default 60)
            output_file: Output HTML file path
            map_type: Base map type ("sectional" or "satellite")
            los_start: Start of LOS event (create_time); trail drawn bold during event
            los_end: End of LOS event (last_time)

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

        # Add static trail lines (always visible), with interpolated gaps in grey
        self._add_trail_polylines(m, positions1, self.AIRCRAFT1_COLOR,
                                  los_start=los_start, los_end=los_end)
        self._add_trail_polylines(m, positions2, self.AIRCRAFT2_COLOR,
                                  los_start=los_start, los_end=los_end)

        # Build animated point features with altitude labels
        features = []
        features.extend(self._build_features(positions1, self.AIRCRAFT1_COLOR))
        features.extend(self._build_features(positions2, self.AIRCRAFT2_COLOR))

        # Add TimestampedGeoJson layer for animated dots
        TimestampedGeoJson(
            {"type": "FeatureCollection", "features": features},
            period="PT1S",           # 1-second intervals
            duration="PT1S",         # Each point visible for 1 second
            transition_time=100,     # 100ms between frames (10 fps)
            auto_play=False,         # Start paused
            add_last_point=False,    # Don't add default markers
            loop=False,
            max_speed=10,
            loop_button=True,
            time_slider_drag_update=True,
            speed_slider=False
        ).add_to(m)

        # Hide the time display (shows browser local time which is confusing).
        # CSS hides it, JS removes it as a fallback after the control renders.
        hide_time_html = '''
        <style>
        .leaflet-bar-timecontrol a.timecontrol-date,
        a.timecontrol-date {
            display: none !important;
        }
        .leaflet-bar-timecontrol .timecontrol-dateslider .slider {
            width: 100px !important;
        }
        .leaflet-bottom.leaflet-left .leaflet-bar-timecontrol {
            float: right;
        }
        .leaflet-bottom.leaflet-left {
            left: auto !important;
            right: 5px !important;
            bottom: 5px !important;
        }
        .timecontrol-speed:before {
            display: none !important;
        }
        </style>
        <script>
        document.addEventListener('DOMContentLoaded', function() {
            setTimeout(function() {
                document.querySelectorAll('.timecontrol-date, a.timecontrol-date').forEach(function(el) {
                    el.style.display = 'none';
                });
            }, 500);
        });
        </script>
        '''
        m.get_root().html.add_child(folium.Element(hide_time_html))

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
        <div style="position: fixed; bottom: 5px; left: 5px; z-index: 1000;
                    background-color: white; padding: 10px; border-radius: 5px;
                    border: 2px solid gray; font-size: 12px;">
            <b>LOS Event</b><br>
            <span style="color: {self.AIRCRAFT1_COLOR};">●</span> {tail1}<br>
            <span style="color: {self.AIRCRAFT2_COLOR};">●</span> {tail2}<br>
            <small><span style="color:orange;">⭕</span> CPA: {datetime.datetime.utcfromtimestamp(event_time).strftime("%H:%M:%S")} UTC</small><br>
            <small><span style="color:magenta;">▬</span> LOS duration: {int(los_end - los_start) if los_start and los_end else "?"} sec</small><br>
            <small>Total duration shown: {end_time - start_time} sec</small><br>
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
        # first_loc_1/2 carry the resampler-assigned flight_id (e.g. "N12345_1")
        flight1_id = los.first_loc_1.flight
        flight2_id = los.first_loc_2.flight
        event_time = los.cpa_time

        if not flight1_id or not flight2_id:
            print(f"  Skipping: missing flight ID on LOS locations")
            return None

        if output_file is None:
            timestamp = datetime.datetime.utcfromtimestamp(event_time)
            output_file = f"los_{tail1}_{tail2}_{timestamp.strftime('%Y%m%d_%H%M%S')}.html"

        result = self.animate_los(
            flight1_id, flight2_id, event_time,
            window_before, window_after, output_file,
            los_start=los.create_time, los_end=los.last_time
        )

        if result:
            print(f"  Created: {output_file}")

        return result
