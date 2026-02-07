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
from lib.map_elements import LEGEND_HTML, STATIC_LEGEND_HTML, CoordinateDisplay
from postprocessing.hotspot_analyzer import compute_hotspot_heatmap

# Corners of the map
LL_LAT = 40.7126
UR_LAT = 40.8199
LL_LON = -119.3068
UR_LON = -119.0603


class MapVisualizer:
    """
    A class for visualizing airport data points on maps with annotations.

    Accumulates points and annotations, then renders them on a Folium map
    with configurable boundaries and styling.
    """

    def __init__(self, ll_lat=LL_LAT, ur_lat=UR_LAT, ll_lon=LL_LON, ur_lon=UR_LON,
                 legend_html=LEGEND_HTML, map_type="sectional", map_opacity=0.8):
        """
        Initialize the MapVisualizer.

        Args:
            ll_lat: Lower-left latitude boundary
            ur_lat: Upper-right latitude boundary
            ll_lon: Lower-left longitude boundary
            ur_lon: Upper-right longitude boundary
            legend_html: HTML string for the map legend
            map_type: Type of base map to use ("sectional" or "satellite")
            map_opacity: Opacity of the base map (0.0 to 1.0)
        """
        self.ll_lat = ll_lat
        self.ur_lat = ur_lat
        self.ll_lon = ll_lon
        self.ur_lon = ur_lon
        self.legend_html = legend_html
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

    def add_traffic_cloud(self, m, points, opacity=0.15, radius=30):
        """Add traffic point cloud as semi-transparent circles.

        Args:
            m: Folium map object
            points: List of (lat, lon, alt) tuples
            opacity: Circle opacity (0.0-1.0)
            radius: Circle radius in meters
        """
        if not points:
            return

        cloud_group = folium.FeatureGroup(name="Traffic Cloud", show=True)

        for lat, lon, alt in points:
            folium.Circle(
                location=[lat, lon],
                radius=radius,
                color='blue',
                fill=True,
                fill_color='blue',
                fill_opacity=opacity,
                opacity=opacity,
                weight=0  # No border
            ).add_to(cloud_group)

        cloud_group.add_to(m)
        print(f"Added traffic cloud with {len(points):,} points")

    def visualize(self, vectorlist=None, output_file="airport_map.html",
                  open_in_browser=True, map_image=None, overlay_image=None,
                  geojson_file=None,
                  enable_heatmap=False, native_heatmap=False, heatmap_bandwidth=None,
                  heatmap_radius=20, heatmap_blur=25, heatmap_min_opacity=0.3,
                  traffic_cloud=None, cloud_opacity=0.15, cloud_radius=30):
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
        if not self.points and not traffic_cloud:
            raise ValueError("Points list cannot be empty and no traffic cloud provided")

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

        # Add traffic cloud BEFORE LOS points (background layer)
        if traffic_cloud:
            self.add_traffic_cloud(m, traffic_cloud, cloud_opacity, cloud_radius)

        # Plot LOS points with annotations and hide functionality (if any)
        if self.points:
            # Create a feature group to hold all point markers
            points_group = folium.FeatureGroup(name="LOS Points")

            for idx, ((lat, lon), annotation) in enumerate(zip(self.points, self.annotations)):
                color = self._get_point_color(annotation)
                # Add hide link to annotation
                hide_link = f'<br><a href="#" onclick="hidePoint({idx}); return false;" style="color:gray;font-size:11px;">Hide this point</a>'
                popup_html = annotation + hide_link

                circle = folium.Circle(
                    location=[lat, lon],
                    radius=85,
                    color=color,
                    fill=True,
                    fill_color=color,
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
        # Build point data array for native heatmap
        points_json = [[p[0], p[1]] for p in self.points] if self.points else []
        # Get the Folium map's JavaScript variable name
        map_name = m.get_name()

        hide_script = f"""
        <script>
        var hiddenPoints = [];
        var allPoints = {points_json};
        var nativeHeatmapLayer = null;

        function getMap() {{
            // Folium map variable name is known
            return {map_name};
        }}

        function rebuildNativeHeatmap() {{
            var map = getMap();
            if (!map || !nativeHeatmapLayer) return;

            // Get visible points (exclude hidden)
            var visiblePoints = allPoints.filter(function(_, idx) {{
                return hiddenPoints.indexOf(idx) === -1;
            }});

            // Remove old heatmap
            map.removeLayer(nativeHeatmapLayer);

            // Create new heatmap with visible points only
            if (visiblePoints.length > 0) {{
                nativeHeatmapLayer = L.heatLayer(visiblePoints, {{
                    radius: {heatmap_radius},
                    blur: {heatmap_blur},
                    minOpacity: {heatmap_min_opacity},
                    gradient: {{0.0: 'blue', 0.3: 'cyan', 0.5: 'lime', 0.7: 'yellow', 0.9: 'orange', 1.0: 'red'}}
                }});
                nativeHeatmapLayer.addTo(map);
                console.log('Rebuilt heatmap with ' + visiblePoints.length + ' points');
            }}
        }}

        function hidePoint(idx) {{
            var map = getMap();
            if (!map) {{
                console.error('Could not find map');
                return;
            }}

            var found = false;
            map.eachLayer(function(layer) {{
                // Check multiple ways popup content might be stored
                var content = null;
                if (layer._popup) {{
                    var popupContent = layer._popup._content;
                    // Content might be a DOM element or string
                    if (popupContent) {{
                        if (typeof popupContent === 'string') {{
                            content = popupContent;
                        }} else if (popupContent.innerHTML) {{
                            content = popupContent.innerHTML;
                        }} else if (popupContent.outerHTML) {{
                            content = popupContent.outerHTML;
                        }}
                    }}
                }}
                if (content && content.includes('hidePoint(' + idx + ')')) {{
                    map.removeLayer(layer);
                    hiddenPoints.push(idx);
                    found = true;
                    console.log('Hidden point ' + idx);
                }}
            }});

            if (!found) {{
                console.log('Point ' + idx + ' not found in layers');
            }}

            // Rebuild native heatmap if it exists
            if (nativeHeatmapLayer) {{
                rebuildNativeHeatmap();
            }}

            // Close any open popup
            map.closePopup();
        }}

        function showAllPoints() {{
            // Reload the page to restore all points
            location.reload();
        }}
        </script>
        """
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
        if native_heatmap:
            # Add a HeatMap to force Folium to include the Leaflet.heat library
            # We create it with the actual data - it will be our initial heatmap
            HeatMap(
                [[p[0], p[1]] for p in self.points],
                name="Dynamic Heatmap",
                radius=heatmap_radius,
                blur=heatmap_blur,
                min_opacity=heatmap_min_opacity,
                gradient={
                    '0.0': 'blue',
                    '0.3': 'cyan',
                    '0.5': 'lime',
                    '0.7': 'yellow',
                    '0.9': 'orange',
                    '1.0': 'red'
                }
            ).add_to(m)

            # Add JavaScript to find and track the heatmap layer for rebuilding on hide
            native_heatmap_script = """
            <script>
            document.addEventListener('DOMContentLoaded', function() {{
                // Wait for map to be ready
                setTimeout(function() {{
                    var map = getMap();
                    if (map) {{
                        // Find the existing heatmap layer created by Folium
                        map.eachLayer(function(layer) {{
                            if (layer._heat) {{
                                nativeHeatmapLayer = layer;
                                console.log('Found native heatmap layer with ' + allPoints.length + ' points');
                            }}
                        }});
                    }}
                }}, 500);
            }});
            </script>
            """
            m.get_root().html.add_child(folium.Element(native_heatmap_script))
            print(f"Native heatmap enabled with {len(self.points)} points (updates on hide)")

        # Add a legend to the map
        m.get_root().html.add_child(folium.Element(self.legend_html))

        # Add dynamic coordinate display that updates on zoom/pan
        m.add_child(CoordinateDisplay())

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
                        help="Path to CSV file with traffic point cloud samples (lat,lon,alt)")
    parser.add_argument("--cloud-opacity", type=float, default=0.2,
                        help="Traffic cloud point opacity, range 0.0-1.0 (default: 0.5)")
    parser.add_argument("--cloud-radius", type=int, default=30,
                        help="Traffic cloud point radius in meters (default: 30)")
    parser.add_argument("--sw", type=str, default=None,
                        help="Southwest corner as 'lat,lon' (e.g., '37.0,-122.5')")
    parser.add_argument("--ne", type=str, default=None,
                        help="Northeast corner as 'lat,lon' (e.g., '37.5,-122.0')")
    parser.add_argument("--output", type=str, default="airport_map.html",
                        help="Output HTML file path (default: airport_map.html)")
    parser.add_argument("--no-browser", action="store_true",
                        help="Don't open the map in a web browser")
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

                annotation = f"{datestr} {tail1}/{tail2} <b>Alt:</b> {altitude}"
                if los_type:
                    annotation += f" <b>type:</b> {los_type}"
                if phase:
                    annotation += f" <b>phase:</b> {phase}"
                if latdist:
                    annotation += f" <b>lat sep:</b> {float(latdist):.2f}nm"
                if altdist:
                    annotation += f" <b>alt sep:</b> {altdist}ft"
                annotation += f" <b><a href='{link}' target='_blank'>REPLAY</a></b>"
                if animation:
                    annotation += f" <b><a href='{animation}' target='_blank'>ANIMATION</a></b>"

                visualizer.add_point((float(lat), float(lon)), annotation)
                print(f"Read point {ctr} at {lat} {lon} alt: {altitude} datestr:{datestr}")
                ctr += 1
            except (ValueError, IndexError) as e:
                print(f"Parse error on row: {row} - {e}")

    # Load traffic samples if provided
    traffic_cloud = None
    if args.traffic_samples and os.path.exists(args.traffic_samples):
        print(f"Loading traffic samples from {args.traffic_samples}...")
        traffic_cloud = []

        with open(args.traffic_samples, 'r') as f:
            for line in f:
                try:
                    lat, lon, alt = line.strip().split(',')
                    traffic_cloud.append((float(lat), float(lon), float(alt)))
                except (ValueError, IndexError):
                    continue  # Skip malformed lines

        print(f"Loaded {len(traffic_cloud):,} traffic points")

    print(f"Visualizing {ctr} LOS events.")
    if ctr == 0 and not traffic_cloud:
        print("⚠️ No LOS events or traffic cloud to visualize")
        sys.exit(0)
    elif ctr == 0:
        print("⚠️ No LOS events found, but will render traffic cloud")

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
        traffic_cloud=traffic_cloud,
        cloud_opacity=args.cloud_opacity,
        cloud_radius=args.cloud_radius
    )
