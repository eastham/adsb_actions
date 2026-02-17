"""Convert CSV from analyze_from_files.py to a map with points and annotations.

This script reads a file containing CSV lines (including adsb_actions output)
from stdin, extracts latitude and longitude coordinates, and plots them 
on a base map using Folium. The map can be saved as an HTML file and 
optionally opened in a web browser.

NOTE: uses geopandas, which is typically installed via conda, not pip.  
once installed, command line is something like:

conda activate pyproj_env
cat 2022.csv | python3 ../visualizer.py  --map-image ./map_anno.png
"""

import argparse
import folium
from folium.plugins import HeatMap
import webbrowser
import os
import sys
import csv
from lib.map_elements import (
    CoordinateDisplay,
    build_hide_script, NATIVE_HEATMAP_SCRIPT,
    build_busyness_html, build_help_html,
    build_quality_indicator_json,
)
from postprocessing.hotspot_analyzer import compute_hotspot_heatmap

# Corners of the map
LL_LAT = 40.7126
UR_LAT = 40.8199
LL_LON = -119.3068
UR_LON = -119.0603

# Feature flags
SHOW_ANALYSIS_WINDOW = False  # Lower-left analysis window (for heatmap analysis)


class MapVisualizer:
    """
    A class for visualizing airport data points on maps with annotations.

    Accumulates points and annotations, then renders them on a Folium map
    with configurable boundaries and styling.
    """

    def __init__(self, ll_lat=LL_LAT, ur_lat=UR_LAT, ll_lon=LL_LON, ur_lon=UR_LON,
                 map_type="sectional", map_opacity=0.8):
        """
        Initialize the MapVisualizer.

        Args:
            ll_lat: Lower-left latitude boundary
            ur_lat: Upper-right latitude boundary
            ll_lon: Lower-left longitude boundary
            ur_lon: Upper-right longitude boundary
            map_type: Type of base map to use ("sectional" or "satellite")
            map_opacity: Opacity of the base map (0.0 to 1.0)
        """
        self.ll_lat = ll_lat
        self.ur_lat = ur_lat
        self.ll_lon = ll_lon
        self.ur_lon = ur_lon
        self.map_type = map_type
        self.map_opacity = map_opacity
        self.points = []
        self.annotations = []
        self.qualities = []  # Track quality for each point
        self.links = []  # Track links for each point

    def add_point(self, point, annotation, links, quality='high'):
        """
        Add a point/event with annotation to be visualized.

        Args:
            point: Tuple of (latitude, longitude)
            annotation: String description/annotation for the point
            links: String of links for the point
            quality: Quality level ('high', 'medium', 'low')
        """
        self.points.append(point)
        self.annotations.append(annotation)
        self.links.append(links)
        self.qualities.append(quality)

    def clear(self):
        """Clear all accumulated points and annotations."""
        self.points.clear()
        self.annotations.clear()
        self.qualities.clear()

    def _get_point_color(self, quality):
        """
        Determine point color based on event quality.

        Args:
            quality: Quality level ('vhigh', 'high', 'medium', 'low')

        Returns:
            Color string for the point marker
        """
        if quality == 'vhigh':
            return "#ff00ff"
        elif quality == 'high':
            return "orange"
        elif quality == 'medium':
            return "yellow"
        elif quality == 'low':
            return "green"
        else:
            return "green"  # Default to blue for unknown quality

    def add_traffic_tile_overlay(self, m, tile_dir, zoom=None, opacity=0.7,
                                 radius_nm=100, traffic_label=None):
        """Add pre-rendered traffic density tiles as a TileLayer using relative URLs.

        Creates a TileLayer that references tiles via relative file paths, avoiding
        base64 embedding that bloats HTML file size.

        Args:
            m: Folium map object
            tile_dir: Path to tile directory containing {z}/{x}/{y}.png
            zoom: Tile zoom level to load (auto-detected from directory if None)
            opacity: Overlay opacity (0.0-1.0)
            radius_nm: Load tiles within this radius (nm) of map center (unused with TileLayer)
        """
        from pathlib import Path
        import os

        tile_dir = Path(tile_dir)

        # Find available zoom levels
        available_zooms = sorted([
            int(d.name) for d in tile_dir.iterdir()
            if d.is_dir() and d.name.isdigit()
        ])
        if not available_zooms:
            print(f"No tile zoom directories found in {tile_dir}")
            return

        min_zoom = min(available_zooms)
        max_zoom = max(available_zooms)
        print(f"Available tile zoom levels: {available_zooms}")

        # Create relative path from HTML output location to tile directory
        # Tiles will be referenced as: tiles/{z}/{x}/{y}.png (matching symlink name)
        tile_url = "tiles/{z}/{x}/{y}.png"

        # Add TileLayer with relative file:// URLs
        # Use minNativeZoom/maxNativeZoom so Leaflet scales tiles for ±1 zoom
        folium.TileLayer(
            tiles=tile_url,
            attr="Traffic Density",
            name="Traffic Density",
            opacity=opacity,
            min_zoom=max(0, min_zoom - 1),
            max_zoom=15,
            min_native_zoom=min_zoom,
            max_native_zoom=max_zoom,
            overlay=True,
            control=True
        ).add_to(m)

        # Add opacity slider control for traffic tiles
        opacity_slider_html = """
        <div id="opacity-controls-box" style="position: fixed;
                    top: 10px;
                    right: 10px;
                    width: 250px;
                    background-color: white;
                    border: 2px solid grey;
                    z-index: 9999;
                    padding: 10px;
                    border-radius: 5px;
                    box-shadow: 2px 2px 6px rgba(0,0,0,0.3);">
            <label for="traffic-opacity-slider" style="font-weight: bold; font-size: 12px;">
                <span style="color: #34c0eb; font-size: 20px;">■</span><span style="color: #0000ff; font-size: 20px;">■</span><span style="color: #7434eb; font-size: 20px;">■</span> Traffic Layer Opacity: <span id="opacity-value">""" + str(int(opacity * 100)) + """</span>%
            </label><br>
            <input type="range"
                   id="traffic-opacity-slider"
                   min="0"
                   max="100"
                   value=\"""" + str(int(opacity * 100)) + """\"
                   style="width: 100%; margin-top: 5px;">"""
        if traffic_label:
            opacity_slider_html += f"""
                <span style=\"font-size: 10px;\">Data: {traffic_label}</span>"""
        opacity_slider_html += """
        </div>
        <script>
            // Wait for map to be ready
            setTimeout(function() {
                var slider = document.getElementById('traffic-opacity-slider');
                var valueDisplay = document.getElementById('opacity-value');

                slider.oninput = function() {
                    valueDisplay.innerHTML = this.value;
                    var opacityValue = this.value / 100.0;

                    // Find the Traffic Density layer and update opacity
                    document.querySelectorAll('.leaflet-tile-pane .leaflet-layer').forEach(function(layer) {
                        var img = layer.querySelector('img');
                        if (img && img.src.match(/tiles\/\d+\/\d+\/\d+\.png/)) {
                            layer.style.opacity = opacityValue;
                        }
                    });
                };
            }, 1000);
        </script>
        """
        m.get_root().html.add_child(folium.Element(opacity_slider_html))

        print(f"Added traffic tile layer with zoom levels {min_zoom}-{max_zoom}")
        print(f"  Tiles will be loaded from: {tile_url}")
        print(f"  Note: Tile directory must be in same location as HTML file")

    def add_traffic_tracks(self, m, tracks, opacity=0.5):
        """Add traffic tracks as semi-transparent polylines.

        Args:
            m: Folium map object
            tracks: List of tracks, each track is a list of [lat, lon, alt] coordinates
            opacity: Line opacity (0.0-1.0)
        """
        if not tracks:
            return

        tracks_group = folium.FeatureGroup(name="Traffic Tracks", show=True)
        total_points = 0

        for track in tracks:
            # Convert [[lat, lon, alt], ...] to [[lat, lon], ...]
            coords = [[point[0], point[1]] for point in track]
            total_points += len(coords)

            # Draw the polyline
            folium.PolyLine(
                locations=coords,
                color='#4488ff',
                weight=3,
                opacity=opacity
            ).add_to(tracks_group)

        tracks_group.add_to(m)
        print(f"Added {len(tracks):,} traffic tracks with {total_points:,} points")

    def add_analysis_radius_circle(self, m, center_lat, center_lon, radius_nm):
        """Add a blue circle showing the analysis radius around the airport.

        Args:
            m: Folium map object
            center_lat: Center latitude
            center_lon: Center longitude
            radius_nm: Radius in nautical miles
        """
        # Convert nautical miles to meters (1 nm = 1852 meters)
        radius_meters = radius_nm * 1852

        folium.Circle(
            location=[center_lat, center_lon],
            radius=radius_meters,
            color='blue',
            fill=False,
            weight=2,
            opacity=0.6,
            popup=f"Analysis radius: {radius_nm}nm"
        ).add_to(m)

        print(f"Added analysis radius circle: {radius_nm}nm at ({center_lat}, {center_lon})")

    def visualize(self, vectorlist=None, output_file="airport_map.html",
                  open_in_browser=True, map_image=None, overlay_image=None,
                  geojson_file=None,
                  enable_heatmap=False, native_heatmap=False, heatmap_bandwidth=None,
                  heatmap_radius=20, heatmap_blur=25, heatmap_min_opacity=0.3,
                  traffic_tracks=None, track_opacity=0.5,
                  traffic_tile_dir=None,
                  busyness_data=None, data_quality=None,
                  analysis_radius_nm=None, analysis_center_lat=None, analysis_center_lon=None,
                  airport_name=None, traffic_label=None, heatmap_label=None):
        """
        Generate and save the visualization map.

        Args:
            vectorlist: Optional list of polygons to draw (e.g., runway, airport boundary)
            output_file: Path to save the HTML map
            open_in_browser: If True, opens the map in the default web browser
            map_image: Path to the static PNG image to use as the map background
            overlay_image: Path to a transparent PNG image to overlay on top of the map

        Raises:
            ValueError: If no points have been added or points/annotations length mismatch
        """
        if not self.points and not traffic_tracks and not traffic_tile_dir:
            raise ValueError("Points list cannot be empty and no traffic tracks provided")

        if self.points and len(self.points) != len(self.annotations):
            raise ValueError(
                "Points list and annotations list must have the same length.")

        center_lat = (self.ll_lat + self.ur_lat) / 2
        center_lon = (self.ll_lon + self.ur_lon) / 2

        # Create a Folium map centered on the airport
        if map_image:
            # Use a static PNG image as the map background
            m = folium.Map(location=[center_lat, center_lon], zoom_start=13, tiles=None,
                           zoom_control=False)

            folium.raster_layers.ImageOverlay(
                image=map_image,
                bounds=[[self.ll_lat, self.ll_lon], [self.ur_lat, self.ur_lon]],
                opacity=self.map_opacity
            ).add_to(m)
        elif self.map_type == "satellite":
            # Use satellite imagery
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=13,
                tiles=None,
                zoom_control=False
            )
            folium.TileLayer(
                tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
                attr="Esri World Imagery",
                opacity=self.map_opacity
            ).add_to(m)
        else:
            # Default to VFR Sectional
            # max_native_zoom=11 tells Leaflet to upscale tiles when zooming past level 11
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=13,
                tiles=None,
                zoom_control=False
            )
            folium.TileLayer(
                tiles="https://tiles.arcgis.com/tiles/ssFJjBXIUyZDrSYZ/arcgis/rest/services/VFR_Sectional/MapServer/tile/{z}/{y}/{x}",
                attr="Esri VFR Sectional",
                opacity=self.map_opacity,
                max_native_zoom=11,
                max_zoom=18
            ).add_to(m)

        # Add zoom control at bottom-left (disabled default top-left above)
        from branca.element import MacroElement, Template
        zoom_bl = MacroElement()
        zoom_bl._template = Template(
            '{% macro script(this, kwargs) %}'
            'L.control.zoom({position: "bottomleft"}).addTo({{this._parent.get_name()}});'
            '{% endmacro %}'
        )
        zoom_bl.add_to(m)

        print("Map created.")

        # Draw the airport boundary
        if vectorlist:
            for vectors in vectorlist:
                folium.Polygon(locations=vectors, color="black",
                             fill=False, tooltip="Airport Boundary").add_to(m)
        print("Boundary drawn.")

        # Load GeoJSON features if provided
        if geojson_file:
            import json
            with open(geojson_file, 'r') as f:
                geojson_data = json.load(f)

            # Separate labels (Points) from other features
            labels = []
            other_features = []
            for feature in geojson_data.get('features', []):
                if feature.get('geometry', {}).get('type') == 'Point':
                    labels.append(feature)
                else:
                    other_features.append(feature)

            # Add non-point features via GeoJson
            if other_features:
                other_geojson = {'type': 'FeatureCollection', 'features': other_features}
                folium.GeoJson(
                    other_geojson,
                    name="Features",
                    tooltip=folium.GeoJsonTooltip(fields=['name'], aliases=[''])
                ).add_to(m)

            # Add labels as DivIcon markers (text only, no icon)
            for label in labels:
                coords = label['geometry']['coordinates']
                props = label.get('properties', {})
                name = props.get('name', '')
                rotation = props.get('rotation', 0)
                folium.Marker(
                    location=[coords[1], coords[0]],  # GeoJSON is [lon, lat]
                    icon=folium.DivIcon(
                        html=f'<div style="font-size: 16px; font-weight: bold; color: black; white-space: nowrap; transform: rotate({rotation}deg); transform-origin: center;">{name}</div>'
                    )
                ).add_to(m)

            print(f"GeoJSON features loaded from: {geojson_file}")

        # Add traffic density tile overlay (pre-rendered PNG tiles)
        if traffic_tile_dir:
            self.add_traffic_tile_overlay(m, traffic_tile_dir,
                                          traffic_label=traffic_label)

        # Add traffic tracks BEFORE LOS points (background layer)
        if traffic_tracks:
            self.add_traffic_tracks(m, traffic_tracks, track_opacity)

        # Add analysis radius circle if metadata is available
        if analysis_radius_nm and analysis_center_lat and analysis_center_lon:
            self.add_analysis_radius_circle(m, analysis_center_lat, analysis_center_lon, analysis_radius_nm)

        # Plot LOS points with annotations and hide functionality (if any)
        if self.points:
            # Create a feature group to hold all point markers
            points_group = folium.FeatureGroup(name="LOS Points")

            for idx, ((lat, lon), annotation, links, quality) in enumerate(zip(self.points, self.annotations, self.links, self.qualities)):
                color = self._get_point_color(quality)
                hide_link = f' - <b><a href="#" onclick="hidePoint({idx}); return false;">Hide this event from heatmap</a></b>'
                # add horizontal rule with minimal vertical spacing
                popup_html = annotation + "<hr style='margin: 5px 0; border-top: 1px solid #ccc;'>"
                # Insert hide_link right after the adsb.lol replay link, before any iframe
                links_with_hide = links.replace('</a></b><div', '</a></b>' + hide_link + '<div')
                if links_with_hide == links:
                    # No iframe present, just append hide_link
                    links_with_hide = links + hide_link
                popup_html += "<div style='text-align: center;'>" + links_with_hide + "</div>"

                circle = folium.CircleMarker(
                    location=[lat, lon],
                    radius=4,  # pixels, not meters - consistent size at all zoom levels
                    color='black',  # Border color
                    weight=1,  # Border width in pixels
                    fill=True,
                    fill_color=color,
                    fill_opacity=1.0,
                    opacity=1.0,
                    popup=folium.Popup(popup_html, max_width=500)
                )
                # Store index as a property for JavaScript access
                circle._name = f"point_{idx}"
                circle.add_to(points_group)

            points_group.add_to(m)
            print("Points plotted.")
        else:
            print("No LOS points to plot (traffic cloud only)")

        # Add JavaScript for hiding points (only if we have points)
        points_json = [[p[0], p[1]] for p in self.points] if self.points else []
        qualities_json = list(self.qualities) if self.qualities else []
        map_name = m.get_name()
        # Use the same radius/blur/max as the native heatmap (hardcoded 25/35/1.0)
        hide_script = build_hide_script(points_json, qualities_json, map_name,
                                        heatmap_radius=25, heatmap_blur=35,
                                        heatmap_min_opacity=heatmap_min_opacity,
                                        heatmap_max=1.0)
        m.get_root().html.add_child(folium.Element(hide_script))

        # Add overlay image if provided
        if overlay_image:
            folium.raster_layers.ImageOverlay(
                image=overlay_image,
                bounds=[[self.ll_lat, self.ll_lon], [self.ur_lat, self.ur_lon]],
                opacity=0.8,
                zindex=500
            ).add_to(m)
            print(f"Overlay image added: {overlay_image}")

        # Add heatmap layer if enabled (only high and very high quality events)
        if enable_heatmap:
            # Filter to only high and very high quality events
            high_quality_points = [
                p for p, q in zip(self.points, self.qualities) if q in ['high', 'vhigh', 'medium']
            ]
            # Compute heatmap using KDE from external module
            # Pass bounds=None to auto-compute from data with padding
            heatmap_data = compute_hotspot_heatmap(
                high_quality_points,
                bounds=None,
                bandwidth=heatmap_bandwidth
            )

            if heatmap_data:
                HeatMap(
                    heatmap_data,
                    name="LOS Hotspots",
                    radius=heatmap_radius,
                    blur=heatmap_blur,
                    min_opacity=heatmap_min_opacity,
                    max_zoom=13,
                    gradient={
                        '0.0': 'green',
                        '0.5': 'yellow',
                        '1.0': 'red'
                    }
                ).add_to(m)

                # Add layer control to toggle heatmap
                folium.LayerControl().add_to(m)
                print(f"Heatmap layer added with {len(heatmap_data)} points (high+vhigh quality only, {len(high_quality_points)} of {len(self.points)} total)")

        # Add native heatmap if enabled (uses raw points, recomputes on hide)
        # Only include high and very high quality events in the heatmap
        if native_heatmap and self.points:
            heatmap_group = folium.FeatureGroup(name="Dynamic Heatmap", show=True)
            # Filter to only high and very high quality events
            high_quality_points = [
                [p[0], p[1]] for p, q in zip(self.points, self.qualities) if q in ['high', 'vhigh', 'medium']
            ]
            # Blue-based color scheme to avoid conflict with red-yellow-green traffic tiles
            # Use larger radius and max parameter to make zoom-independent
            HeatMap(
                high_quality_points,
                radius=25,
                blur=35,
                min_opacity=heatmap_min_opacity,
                max=1.0,  # Maximum point intensity (affects color mapping)
                gradient={
                    '0.0': 'green',
                    '0.5': 'yellow',
                    '1.0': 'red'
                }
            ).add_to(heatmap_group)
            heatmap_group.add_to(m)

            m.get_root().html.add_child(folium.Element(NATIVE_HEATMAP_SCRIPT))

            # Add heatmap opacity slider - appends into existing box or creates new one
            quality_json = build_quality_indicator_json(data_quality)
            heatmap_opacity_slider_html = """
            <script>
                setTimeout(function() {
                    var qualityData = """ + quality_json + """;

                    // Find or create the opacity controls box
                    var box = document.getElementById('opacity-controls-box');
                    if (!box) {
                        box = document.createElement('div');
                        box.id = 'opacity-controls-box';
                        box.style.cssText = 'position: fixed; top: 10px; right: 10px; width: 250px; background-color: white; border: 2px solid grey; z-index: 9999; padding: 10px; border-radius: 5px; box-shadow: 2px 2px 6px rgba(0,0,0,0.3);';
                        document.body.appendChild(box);
                    }

                    // Add separator if box already has content
                    if (box.children.length > 0) {
                        var hr = document.createElement('hr');
                        hr.style.cssText = 'margin: 8px 0; border: none; border-top: 1px solid #ccc;';
                        box.appendChild(hr);
                    }

                    // Add heatmap slider controls
                    var label = document.createElement('label');
                    label.htmlFor = 'heatmap-opacity-slider';
                    label.style.cssText = 'font-weight: bold; font-size: 12px;';
                    label.innerHTML = '<span style="color: green; font-size: 20px; text-shadow: 0 0 1px black;">●</span><span style="color: yellow; font-size: 20px;">●</span><span style="color: orange; font-size: 20px;">●</span><span style="color: #ff00ff; font-size: 20px;">●</span> LOS Heatmap Opacity: <span id="heatmap-opacity-value">60</span>%';
                    box.appendChild(label);
                    box.appendChild(document.createElement('br'));

                    var slider = document.createElement('input');
                    slider.type = 'range';
                    slider.id = 'heatmap-opacity-slider';
                    slider.min = '0';
                    slider.max = '100';
                    slider.value = '60';
                    slider.style.cssText = 'width: 100%; margin-top: 5px;';
                    box.appendChild(slider);

                    var valueDisplay = document.getElementById('heatmap-opacity-value');
                    slider.oninput = function() {
                        valueDisplay.innerHTML = this.value;
                        var opacityValue = this.value / 100.0;
                        if (nativeHeatmapLayer && nativeHeatmapLayer._canvas) {
                            nativeHeatmapLayer._canvas.style.opacity = opacityValue;
                        }
                    };
                    """
            if heatmap_label:
                heatmap_opacity_slider_html += """
                    var heatmap_label = document.createElement('label');
                    heatmap_label.innerHTML = "<span style='font-size: 10px;'>Data: """+heatmap_label+"""</span>";
                    box.appendChild(heatmap_label);"""

            heatmap_opacity_slider_html += """
                    // Add data quality indicator if available
                    if (qualityData) {
                        var qualityDiv = document.createElement('div');
                        qualityDiv.style.cssText = 'font-size: 11px; color: #555; position: relative; user-select: none;';
                        var qualitySpan = document.createElement('span');
                        qualitySpan.style.cssText = 'color: blue; text-decoration: underline; cursor: pointer;';
                        qualitySpan.textContent = qualityData.label;
                        var tipDiv = document.createElement('div');
                        tipDiv.style.cssText = 'display:none; background:#fff; border:1px solid #ccc; padding:6px 8px; margin-top:4px; font-size:11px; color:#333; border-radius:4px; box-shadow:0 2px 6px rgba(0,0,0,0.15);';
                        tipDiv.textContent = qualityData.tooltip;
                        qualitySpan.onclick = function() {
                            tipDiv.style.display = tipDiv.style.display === 'block' ? 'none' : 'block';
                        };
                        qualityDiv.innerHTML = '<span style="font-weight: bold;">ADS-B Data Quality:</span> ' +
                            '<span style="color: ' + qualityData.color + '; font-size: 24px; vertical-align: middle;">●</span> ';
                        qualityDiv.appendChild(qualitySpan);
                        qualityDiv.appendChild(tipDiv);
                        box.appendChild(qualityDiv);
                    }
                }, 1000);
            </script>
            """
            m.get_root().html.add_child(folium.Element(heatmap_opacity_slider_html))

            high_quality_count = sum(1 for q in self.qualities if q in ['high', 'vhigh'])
            print(f"Native heatmap enabled with {high_quality_count} high+vhigh quality points (of {len(self.points)} total, updates on hide)")

        # Add busyness chart panel if data is available
        if busyness_data:
            m.get_root().html.add_child(folium.Element(build_busyness_html(busyness_data)))
            print(f"Busyness chart added to map")

        # Add dynamic coordinate display (hidden behind feature flag for analysis use)
        if SHOW_ANALYSIS_WINDOW:
            m.add_child(CoordinateDisplay())

        # Add help window with keyboard shortcuts and generation info
        m.get_root().html.add_child(folium.Element(build_help_html(airport_name)))

        # Save the map to an HTML file
        m.save(output_file)
        print(f"Map saved to {output_file}")

        # Create symlink to traffic tiles if specified
        if traffic_tile_dir:
            from pathlib import Path
            output_path = Path(output_file).resolve()
            output_dir = output_path.parent
            tile_src = Path(traffic_tile_dir).resolve()
            tile_link = output_dir / "tiles"

            # Create or update symlink
            if tile_link.exists() or tile_link.is_symlink():
                if tile_link.is_symlink():
                    # Check if it points to the right place
                    if tile_link.resolve() != tile_src:
                        tile_link.unlink()
                        tile_link.symlink_to(tile_src)
                        print(f"Updated symlink: {tile_link} -> {tile_src}")
                    else:
                        print(f"Symlink already exists: {tile_link} -> {tile_src}")
                else:
                    print(f"Warning: {tile_link} exists but is not a symlink, skipping")
            else:
                tile_link.symlink_to(tile_src)
                print(f"Created symlink: {tile_link} -> {tile_src}")

        if open_in_browser:
            webbrowser.open("file://" + os.path.realpath(output_file))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Visualize points on a map.")
    parser.add_argument("--map-image", type=str, default=None,
                        help="Path to a static PNG image for the map background. If not specified, uses --map-type.")
    parser.add_argument("--map-type", type=str, default="sectional",
                        choices=["sectional", "satellite"],
                        help="Type of base map to use: 'sectional' (VFR chart) or 'satellite' (imagery)")
    parser.add_argument("--map-opacity", type=float, default=0.5,
                        help="Opacity for the base map, range 0.0-1.0 (default: 0.8)")
    parser.add_argument("--overlay-image", type=str, default=None,
                        help="Path to a transparent PNG image to overlay on the map at the defined boundaries")
    parser.add_argument("--geojson", type=str, default=None,
                        help="Path to a GeoJSON file with polygons, lines, and/or labels to overlay")
    parser.add_argument("--enable-heatmap", action="store_true",
                        help="Enable KDE-based heatmap overlay showing LOS hotspots")
    parser.add_argument("--native-heatmap", action="store_true",
                        help="Enable native Folium heatmap that updates when points are hidden")
    parser.add_argument("--heatmap-bandwidth", type=float, default=None,
                        help="KDE bandwidth in degrees (default: auto using Scott's rule)")
    parser.add_argument("--heatmap-radius", type=int, default=20,
                        help="Heatmap point radius (default: 20)")
    parser.add_argument("--heatmap-blur", type=int, default=25,
                        help="Heatmap blur amount (default: 25)")
    parser.add_argument("--heatmap-opacity", type=float, default=0.2,
                        help="Heatmap minimum opacity, range 0.0-1.0 (default: 0.3)")
    parser.add_argument("--traffic-samples", type=str,
                        help="Path to JSON file with traffic tracks (one track per line)")
    parser.add_argument("--traffic-tiles", type=str, default=None,
                        help="Path to traffic tile directory (contains {z}/{x}/{y}.png)")
    parser.add_argument("--track-opacity", type=float, default=0.1,
                        help="Traffic track opacity, range 0.0-1.0 (default: 0.5)")
    parser.add_argument("--sw", type=str, default=None,
                        help="Southwest corner as 'lat,lon' (e.g., '37.0,-122.5')")
    parser.add_argument("--ne", type=str, default=None,
                        help="Northeast corner as 'lat,lon' (e.g., '37.5,-122.0')")
    parser.add_argument("--output", type=str, default="airport_map.html",
                        help="Output HTML file path (default: airport_map.html)")
    parser.add_argument("--no-browser", action="store_true",
                        help="Don't open the map in a web browser")
    parser.add_argument("--busyness-data", type=str, default=None,
                        help="Path to busyness JSON file for traffic chart overlay")
    parser.add_argument("--data-quality", type=str, default=None,
                        help="Path to data quality JSON file for coverage badge")
    parser.add_argument("--airport-name", type=str, default=None,
                        help="Name of the airport (for display purposes only, does not affect map boundaries)")
    parser.add_argument("--traffic-label", type=str, default="",
                        help="Label for traffic data in the busyness chart (e.g., date range)")
    parser.add_argument("--heatmap-label", type=str, default="",
                        help="Label for heatmap data in the map legend")
    args = parser.parse_args()

    # Clamp opacity to valid range
    opacity = max(0.0, min(1.0, args.map_opacity))

    # Parse coordinate bounds if provided
    ll_lat, ur_lat, ll_lon, ur_lon = LL_LAT, UR_LAT, LL_LON, UR_LON
    if args.sw and args.ne:
        try:
            sw_lat, sw_lon = map(float, args.sw.split(','))
            ne_lat, ne_lon = map(float, args.ne.split(','))
            ll_lat, ll_lon = sw_lat, sw_lon
            ur_lat, ur_lon = ne_lat, ne_lon
            print(f"Using bounds: SW=({ll_lat}, {ll_lon}) NE=({ur_lat}, {ur_lon})")
        except ValueError:
            print(f"Warning: Could not parse --sw/--ne coordinates, using defaults", file=sys.stderr)

    # Create visualizer instance
    visualizer = MapVisualizer(
        ll_lat=ll_lat, ur_lat=ur_lat, ll_lon=ll_lon, ur_lon=ur_lon,
        map_type=args.map_type, map_opacity=opacity
    )

    # accept on stdin lines containing "CSV OUTPUT FOR POSTPROCESSING:"
    # Format from los.py:
    # CSV OUTPUT FOR POSTPROCESSING: timestamp,datestring,altdatestring,lat,lon,
    #   alt,flight1,flight2,notused,link,animation,interp,audio,type,phase,,latdist,altdist,
    ctr = 0
    analysis_radius_nm = None
    center_lat = None
    center_lon = None

    for line in sys.stdin:
        # Parse metadata comments (formatted for easy Python parsing)
        if line.startswith("# analysis_radius_nm = "):
            analysis_radius_nm = float(line.split("=")[1].strip())
            continue
        if line.startswith("# center_lat = "):
            center_lat = float(line.split("=")[1].strip())
            continue
        if line.startswith("# center_lon = "):
            center_lon = float(line.split("=")[1].strip())
            continue

        # Strip the "CSV OUTPUT FOR POSTPROCESSING:" prefix and any filename prefix from grep
        if "CSV OUTPUT FOR POSTPROCESSING:" not in line:
            continue
        csv_part = line.split("CSV OUTPUT FOR POSTPROCESSING:")[1].strip()

        # Parse as CSV
        reader = csv.reader([csv_part])
        for row in reader:
            if len(row) < 10:
                print(f"Skipping short row: {row}")
                continue
            try:
                # Fields: timestamp,datestr,altdatestr,lat,lon,alt,flight1,flight2,quality,link,animation,interp,audio,type,phase,quality_explanation,latdist,altdist
                timestamp = row[0]
                datestr = row[1]
                lat = row[3]
                lon = row[4]
                altitude = row[5]
                tail1 = row[6]
                tail2 = row[7]
                quality = row[8] if len(row) > 8 else "high"  # Default to high if missing
                link = row[9]
                animation = row[10] if len(row) > 10 else ""
                los_type = row[13] if len(row) > 13 else ""
                phase = row[14] if len(row) > 14 else ""
                quality_explanation = row[15] if len(row) > 15 else ""
                latdist = row[16] if len(row) > 16 else ""
                altdist = row[17] if len(row) > 17 else ""

                annotation = f"{tail1}/{tail2} Closest Point of Approach (CPA) at: <b>{datestr}</b>  "
                if quality:
                    annotation += f"<br><b>Event Quality:</b> {quality}"
                    if quality_explanation:
                        annotation += f" ({quality_explanation})"
                links = ""
                if animation:
                    links += f"<b><a href='{animation}' target='_blank'>View this preview fullscreen</a></b> - "
                links += f"<b><a href='{link}' target='_blank'>adsb.lol replay</a></b>"
                if animation:
                    links += (f"<div style='margin-top: 6px;'>"
                              f"<iframe src='{animation}' "
                              f"style='width: 460px; height: 300px; border: 1px solid #ccc; "
                              f"border-radius: 3px;'></iframe></div>")

                visualizer.add_point((float(lat), float(lon)), annotation, links, quality=quality)
                print(f"Read point {ctr} at {lat} {lon} alt: {altitude} quality: {quality} datestr:{datestr}")
                ctr += 1
            except (ValueError, IndexError) as e:
                print(f"Parse error on row: {row} - {e}")

    # Load busyness data if provided
    busyness_data = None
    if args.busyness_data and os.path.exists(args.busyness_data):
        import json
        with open(args.busyness_data, 'r') as f:
            busyness_data = json.load(f)
        print(f"Loaded busyness data: {busyness_data.get('numDates', '?')} dates, "
              f"globalMax={busyness_data.get('globalMax', '?')}")

    # Load data quality if provided
    quality_data = None
    if args.data_quality and os.path.exists(args.data_quality):
        import json
        with open(args.data_quality, 'r') as f:
            quality_data = json.load(f)
        print(f"Loaded data quality: {quality_data.get('score', '?')}")

    # Load traffic tracks if provided
    traffic_tracks = None
    if args.traffic_samples and os.path.exists(args.traffic_samples):
        print(f"Loading traffic tracks from {args.traffic_samples}...")
        traffic_tracks = []
        total_points = 0

        import json
        with open(args.traffic_samples, 'r') as f:
            for line in f:
                try:
                    # Each line is a JSON array of [lat, lon, alt] coordinates
                    coords = json.loads(line.strip())
                    if coords and len(coords) >= 2:  # Need at least 2 points for a line
                        traffic_tracks.append(coords)
                        total_points += len(coords)
                except (json.JSONDecodeError, ValueError, IndexError):
                    continue  # Skip malformed lines

        print(f"Loaded {len(traffic_tracks):,} flight tracks with {total_points:,} total points")

    print(f"Visualizing {ctr} LOS events.")
    if ctr == 0 and not traffic_tracks and not args.traffic_tiles:
        print("⚠️ No LOS events or traffic tracks to visualize")
        sys.exit(0)
    elif ctr == 0:
        print("⚠️ No LOS events found, but will render traffic tracks")

    visualizer.visualize(
        vectorlist=None,
        output_file=args.output,
        open_in_browser=not args.no_browser,
        map_image=args.map_image,
        overlay_image=args.overlay_image,
        geojson_file=args.geojson,
        enable_heatmap=args.enable_heatmap,
        native_heatmap=args.native_heatmap,
        heatmap_bandwidth=args.heatmap_bandwidth,
        heatmap_radius=args.heatmap_radius,
        heatmap_blur=args.heatmap_blur,
        heatmap_min_opacity=args.heatmap_opacity,
        traffic_tracks=traffic_tracks,
        track_opacity=args.track_opacity,
        traffic_tile_dir=args.traffic_tiles,
        busyness_data=busyness_data,
        data_quality=quality_data,
        analysis_radius_nm=analysis_radius_nm,
        analysis_center_lat=center_lat,
        analysis_center_lon=center_lon,
        airport_name=args.airport_name,
        heatmap_label=args.heatmap_label,
        traffic_label=args.traffic_label
    )
