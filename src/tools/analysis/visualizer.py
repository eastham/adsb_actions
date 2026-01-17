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
from tools.analysis.map_elements import LEGEND_HTML, STATIC_LEGEND_HTML, CoordinateDisplay
from tools.analysis.hotspot_analyzer import compute_hotspot_heatmap

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
            return "blue"
        elif "tbone" in annotation_lower:
            return "orange"
        elif "headon" in annotation_lower:
            return "red"
        else:
            return "green"

    def visualize(self, vectorlist=None, output_file="airport_map.html",
                  open_in_browser=True, map_image=None, overlay_image=None,
                  geojson_file=None,
                  enable_heatmap=False, heatmap_bandwidth=None,
                  heatmap_radius=20, heatmap_blur=25, heatmap_min_opacity=0.3):
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
        if not self.points:
            raise ValueError("Points list cannot be empty")

        if len(self.points) != len(self.annotations):
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
            m = folium.Map(
                location=[center_lat, center_lon],
                zoom_start=13,
                tiles=None
            )
            folium.TileLayer(
                tiles="https://tiles.arcgis.com/tiles/ssFJjBXIUyZDrSYZ/arcgis/rest/services/VFR_Sectional/MapServer/tile/{z}/{y}/{x}",
                attr="Esri VFR Sectional",
                opacity=self.map_opacity
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

        # Plot points with annotations
        for (lat, lon), annotation in zip(self.points, self.annotations):
            color = self._get_point_color(annotation)
            folium.Circle(location=[lat, lon], radius=85, color=color,
                        fill=True, fill_color=color, popup=annotation).add_to(m)
        print("Points plotted.")

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
    parser.add_argument("--map-opacity", type=float, default=80.0,
                        help="Opacity percentage" \
                        " for the base map (0-100, default: 80)")
    parser.add_argument("--overlay-image", type=str, default=None,
                        help="Path to a transparent PNG image to overlay on the map at the defined boundaries")
    parser.add_argument("--geojson", type=str, default=None,
                        help="Path to a GeoJSON file with polygons, lines, and/or labels to overlay")
    parser.add_argument("--enable-heatmap", action="store_true",
                        help="Enable KDE-based heatmap overlay showing LOS hotspots")
    parser.add_argument("--heatmap-bandwidth", type=float, default=None,
                        help="KDE bandwidth in degrees (default: auto using Scott's rule)")
    parser.add_argument("--heatmap-radius", type=int, default=20,
                        help="Heatmap point radius (default: 20)")
    parser.add_argument("--heatmap-blur", type=int, default=25,
                        help="Heatmap blur amount (default: 25)")
    parser.add_argument("--heatmap-opacity", type=float, default=0.3,
                        help="Heatmap minimum opacity 0.0-1.0 (default: 0.3)")
    args = parser.parse_args()

    # Convert percentage to 0.0-1.0 range
    opacity = max(0.0, min(100.0, args.map_opacity)) / 100.0

    # Create visualizer instance
    visualizer = MapVisualizer(map_type=args.map_type, map_opacity=opacity)

    # accept on stdin a csv with rows that look like:
    # timestamp,lat,long,altidude,tail1,tail2
    csv_reader = csv.reader(sys.stdin)

    # Parse the CSV
    ctr = 0
    for row in csv_reader:
        if len(row) < 6:
            continue  # Skip rows with insufficient data
        try:
            #timestamp, _, datestr, lat, lon, altitude, tail1, tail2, _, _, distance, altsep, link, interp, audio, type, phase = row
            timestamp, _, datestr, lat, lon, altitude, tail1, tail2, _, link, interp, audio, type, phase, _, distance, altsep, = row

            annotation = f"{datestr} {tail1}/{tail2} <b>Alt:</b> {altitude} <b>type:</b> {type} <b>phase:</b> {phase}"
            annotation += f" <b>Interp:</b> {interp} <b><a href='{link}' target='_blank'>REPLAY LINK</a></b> <b>ATC audio:</b> {audio}<br>"
            visualizer.add_point((float(lat), float(lon)), annotation)
            print(f"Read point {ctr} at {lat} {lon} alt: {altitude} datestr:{datestr}")
            ctr += 1
        except ValueError as e:
            print(f"Parse error on row: {row} " + str(e) )

    print(f"Visualizing {ctr} points.")
    visualizer.visualize(
        vectorlist=None,
        map_image=args.map_image,
        overlay_image=args.overlay_image,
        geojson_file=args.geojson,
        enable_heatmap=args.enable_heatmap,
        heatmap_bandwidth=args.heatmap_bandwidth,
        heatmap_radius=args.heatmap_radius,
        heatmap_blur=args.heatmap_blur,
        heatmap_min_opacity=args.heatmap_opacity
    )
