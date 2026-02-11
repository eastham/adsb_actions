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

    def add_point(self, point, annotation):
        """
        Add a point with annotation to be visualized.

        Args:
            point: Tuple of (latitude, longitude)
            annotation: String description/annotation for the point
        """
        self.points.append(point)
        self.annotations.append(annotation)

    def clear(self):
        """Clear all accumulated points and annotations."""
        self.points.clear()
        self.annotations.clear()

    def _get_point_color(self, annotation):
        """
        Determine point color based on annotation content.

        Args:
            annotation: String annotation to analyze

        Returns:
            Color string for the point marker
        """
        annotation_lower = annotation.lower()
        if "overtake" in annotation_lower:
            return "red"
        elif "tbone" in annotation_lower:
            return "red"
        elif "headon" in annotation_lower:
            return "red"
        else:
            return "red"  # All LOS events are red

    def add_traffic_tile_overlay(self, m, tile_dir, zoom=None, opacity=0.7,
                                 radius_nm=100):
        """Add pre-rendered traffic density tiles as ImageOverlay layers.

        Loads tiles at multiple zoom levels covering radius_nm around the
        map center. Uses the highest available zoom for the immediate area
        and progressively coarser zooms for the wider region.

        Args:
            m: Folium map object
            tile_dir: Path to tile directory containing {z}/{x}/{y}.png
            zoom: Tile zoom level to load (auto-detected from directory if None)
            opacity: Overlay opacity (0.0-1.0)
            radius_nm: Load tiles within this radius (nm) of map center
        """
        import math
        from pathlib import Path

        tile_dir = Path(tile_dir)

        # Find all available zoom levels
        available_zooms = sorted([
            int(d.name) for d in tile_dir.iterdir()
            if d.is_dir() and d.name.isdigit()
        ], reverse=True)
        if not available_zooms:
            print(f"No tile zoom directories found in {tile_dir}")
            return
        max_zoom = available_zooms[0]
        print(f"Available tile zoom levels: {sorted(available_zooms)}")

        def _latlon_to_tile(lat, lon, z):
            n = 2 ** z
            tx = int((lon + 180.0) / 360.0 * n)
            lat_rad = math.radians(lat)
            ty = int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad))
                       / math.pi) / 2.0 * n)
            return tx, ty

        def _tile_bounds(tx, ty, z):
            n = 2 ** z
            sw_lon = tx / n * 360.0 - 180.0
            ne_lon = (tx + 1) / n * 360.0 - 180.0
            ne_lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * ty / n))))
            sw_lat = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (ty + 1) / n))))
            return [[sw_lat, sw_lon], [ne_lat, ne_lon]]

        traffic_group = folium.FeatureGroup(name="Traffic Density", show=True)
        total_count = 0

        center_lat = (self.ll_lat + self.ur_lat) / 2
        center_lon = (self.ll_lon + self.ur_lon) / 2

        # Load tiles at each zoom level for progressively wider areas
        # Highest zoom: inner area; lower zooms: fill out to radius_nm
        covered = set()  # track parent tiles already covered by children
        for z in available_zooms:
            lat_offset = radius_nm / 60.0
            lon_offset = radius_nm / (60.0 * math.cos(math.radians(center_lat)))

            min_tx, min_ty = _latlon_to_tile(center_lat + lat_offset,
                                              center_lon - lon_offset, z)
            max_tx, max_ty = _latlon_to_tile(center_lat - lat_offset,
                                              center_lon + lon_offset, z)

            count = 0
            for tx in range(min_tx, max_tx + 1):
                for ty in range(min_ty, max_ty + 1):
                    # Skip if a higher-zoom child already covers this area
                    if (z, tx, ty) in covered:
                        continue
                    tile_path = tile_dir / str(z) / str(tx) / f"{ty}.png"
                    if tile_path.exists():
                        bounds = _tile_bounds(tx, ty, z)
                        folium.raster_layers.ImageOverlay(
                            image=str(tile_path),
                            bounds=bounds,
                            opacity=opacity,
                        ).add_to(traffic_group)
                        count += 1
                        # Mark parent as covered so lower zooms skip it
                        if z > available_zooms[-1]:
                            covered.add((z - 1, tx // 2, ty // 2))

            if count > 0:
                total_count += count
                print(f"  Zoom {z}: {count} tiles")

        traffic_group.add_to(m)

        # Add opacity slider control for traffic tiles
        opacity_slider_html = """
        <div style="position: fixed;
                    top: 10px;
                    right: 10px;
                    width: 250px;
                    background-color: white;
                    border: 2px solid grey;
                    z-index: 9999;
                    padding: 10px;
                    border-radius: 5px;
                    box-shadow: 2px 2px 6px rgba(0,0,0,0.3);">
            <label for="traffic-opacity-slider" style="font-weight: bold; font-size: 14px;">
                Traffic Layer Opacity: <span id="opacity-value">""" + str(int(opacity * 100)) + """</span>%
            </label><br>
            <input type="range"
                   id="traffic-opacity-slider"
                   min="0"
                   max="100"
                   value=\"""" + str(int(opacity * 100)) + """\"
                   style="width: 100%; margin-top: 5px;">
        </div>
        <script>
            // Wait for map to be ready
            setTimeout(function() {
                var slider = document.getElementById('traffic-opacity-slider');
                var valueDisplay = document.getElementById('opacity-value');

                slider.oninput = function() {
                    valueDisplay.innerHTML = this.value;
                    var opacityValue = this.value / 100.0;

                    // Find all img elements that are part of ImageOverlay (leaflet-image-layer class)
                    var overlayImages = document.querySelectorAll('.leaflet-image-layer');
                    overlayImages.forEach(function(img) {
                        img.style.opacity = opacityValue;
                    });
                };
            }, 1000);
        </script>
        """
        m.get_root().html.add_child(folium.Element(opacity_slider_html))

        print(f"Added {total_count} traffic density tiles ({radius_nm}nm radius) "
              f"from {tile_dir}")

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

    def visualize(self, vectorlist=None, output_file="airport_map.html",
                  open_in_browser=True, map_image=None, overlay_image=None,
                  geojson_file=None,
                  enable_heatmap=False, native_heatmap=False, heatmap_bandwidth=None,
                  heatmap_radius=20, heatmap_blur=25, heatmap_min_opacity=0.3,
                  traffic_tracks=None, track_opacity=0.5,
                  traffic_tile_dir=None,
                  busyness_data=None, data_quality=None):
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
            m = folium.Map(location=[center_lat, center_lon], zoom_start=13, tiles=None)

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
                tiles=None
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
                tiles=None
            )
            folium.TileLayer(
                tiles="https://tiles.arcgis.com/tiles/ssFJjBXIUyZDrSYZ/arcgis/rest/services/VFR_Sectional/MapServer/tile/{z}/{y}/{x}",
                attr="Esri VFR Sectional",
                opacity=self.map_opacity,
                max_native_zoom=11,
                max_zoom=18
            ).add_to(m)

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
            self.add_traffic_tile_overlay(m, traffic_tile_dir)

        # Add traffic tracks BEFORE LOS points (background layer)
        if traffic_tracks:
            self.add_traffic_tracks(m, traffic_tracks, track_opacity)

        # Plot LOS points with annotations and hide functionality (if any)
        if self.points:
            # Create a feature group to hold all point markers
            points_group = folium.FeatureGroup(name="LOS Points")

            for idx, ((lat, lon), annotation) in enumerate(zip(self.points, self.annotations)):
                color = self._get_point_color(annotation)
                # Add hide link to annotation
                hide_link = f' - <b><a href="#" onclick="hidePoint({idx}); return false;">Hide</a></b>'
                popup_html = annotation + hide_link

                circle = folium.CircleMarker(
                    location=[lat, lon],
                    radius=4,  # pixels, not meters - consistent size at all zoom levels
                    color=color,
                    fill=True,
                    fill_color=color,
                    fill_opacity=1.0,
                    opacity=1.0,
                    popup=folium.Popup(popup_html, max_width=400)
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
        map_name = m.get_name()
        hide_script = build_hide_script(points_json, map_name,
                                        heatmap_radius, heatmap_blur,
                                        heatmap_min_opacity)
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

        # Add heatmap layer if enabled
        if enable_heatmap:
            # Compute heatmap using KDE from external module
            # Pass bounds=None to auto-compute from data with padding
            heatmap_data = compute_hotspot_heatmap(
                self.points,
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
                        '0.0': 'blue',
                        '0.3': 'cyan',
                        '0.5': 'lime',
                        '0.7': 'yellow',
                        '0.9': 'orange',
                        '1.0': 'red'
                    }
                ).add_to(m)

                # Add layer control to toggle heatmap
                folium.LayerControl().add_to(m)
                print(f"Heatmap layer added with {len(heatmap_data)} points")

        # Add native heatmap if enabled (uses raw points, recomputes on hide)
        if native_heatmap and self.points:
            heatmap_group = folium.FeatureGroup(name="Dynamic Heatmap", show=True)
            # Blue-based color scheme to avoid conflict with red-yellow-green traffic tiles
            # Use larger radius and max parameter to make zoom-independent
            HeatMap(
                [[p[0], p[1]] for p in self.points],
                radius=25,
                blur=35,
                min_opacity=heatmap_min_opacity,
                max=1.0,  # Maximum point intensity (affects color mapping)
                gradient={
                    '0.0': 'white',
                    '0.25': 'aqua',
                    '0.5': 'cyan',
                    '0.75': 'blue',
                    '1.0': 'navy'
                }
            ).add_to(heatmap_group)
            heatmap_group.add_to(m)

            m.get_root().html.add_child(folium.Element(NATIVE_HEATMAP_SCRIPT))

            # Add heatmap opacity slider
            heatmap_opacity_slider_html = """
            <div style="position: fixed;
                        top: 90px;
                        right: 10px;
                        width: 250px;
                        background-color: white;
                        border: 2px solid grey;
                        z-index: 9999;
                        padding: 10px;
                        border-radius: 5px;
                        box-shadow: 2px 2px 6px rgba(0,0,0,0.3);">
                <label for="heatmap-opacity-slider" style="font-weight: bold; font-size: 14px;">
                    Heatmap Opacity: <span id="heatmap-opacity-value">60</span>%
                </label><br>
                <input type="range"
                       id="heatmap-opacity-slider"
                       min="0"
                       max="100"
                       value="60"
                       style="width: 100%; margin-top: 5px;">
            </div>
            <script>
                setTimeout(function() {
                    var slider = document.getElementById('heatmap-opacity-slider');
                    var valueDisplay = document.getElementById('heatmap-opacity-value');

                    slider.oninput = function() {
                        valueDisplay.innerHTML = this.value;
                        var opacityValue = this.value / 100.0;

                        // Find heatmap canvas and update its opacity
                        if (nativeHeatmapLayer && nativeHeatmapLayer._canvas) {
                            nativeHeatmapLayer._canvas.style.opacity = opacityValue;
                        }
                    };
                }, 1000);
            </script>
            """
            m.get_root().html.add_child(folium.Element(heatmap_opacity_slider_html))

            print(f"Native heatmap enabled with {len(self.points)} points (updates on hide)")

        # Add busyness chart panel if data is available
        if busyness_data:
            m.get_root().html.add_child(folium.Element(build_busyness_html(busyness_data, data_quality)))
            print(f"Busyness chart added to map")

        # Add dynamic coordinate display (hidden behind feature flag for analysis use)
        if SHOW_ANALYSIS_WINDOW:
            m.add_child(CoordinateDisplay())

        # Add help window with keyboard shortcuts and generation info
        cmd_args = ' '.join(sys.argv[1:]) if len(sys.argv) > 1 else 'none'
        if len(cmd_args) > 200:
            cmd_args = cmd_args[:200] + '...'
        m.get_root().html.add_child(folium.Element(build_help_html(cmd_args)))

        # Save the map to an HTML file
        m.save(output_file)
        print(f"Map saved to {output_file}")

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
    for line in sys.stdin:
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
                # Fields: timestamp,datestr,altdatestr,lat,lon,alt,flight1,flight2,notused,link,animation,interp,audio,type,phase,,latdist,altdist,
                timestamp = row[0]
                datestr = row[1]
                lat = row[3]
                lon = row[4]
                altitude = row[5]
                tail1 = row[6]
                tail2 = row[7]
                link = row[9]
                animation = row[10] if len(row) > 10 else ""
                los_type = row[13] if len(row) > 13 else ""
                phase = row[14] if len(row) > 14 else ""
                latdist = row[16] if len(row) > 16 else ""
                altdist = row[17] if len(row) > 17 else ""

                annotation = f"<b>{datestr}</b> {tail1}/{tail2} "
                if latdist:
                    annotation += f" <b>Lat sep:</b> {float(latdist):.2f}nm"
                if altdist:
                    annotation += f" <b>Alt sep:</b> {altdist}ft"
                if animation:
                    annotation += f"<br><b><a href='{animation}' target='_blank'>Event preview</a></b> - "
                else:
                    annotation += "<br>"
                annotation += f"<b><a href='{link}' target='_blank'>adsb.lol replay</a></b>"

                visualizer.add_point((float(lat), float(lon)), annotation)
                print(f"Read point {ctr} at {lat} {lon} alt: {altitude} datestr:{datestr}")
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
        data_quality=quality_data
    )
