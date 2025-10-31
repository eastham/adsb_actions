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
import geopandas as gpd
from shapely.geometry import Polygon, Point
import webbrowser
import os
import sys
import csv
from map_elements import LEGEND_HTML, STATIC_LEGEND_HTML, CoordinateDisplay

# Corners of the map
LL_LAT = 40.7145599
UR_LAT = 40.8181649
LL_LON = -119.2769929
UR_LON = -119.0903859


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
                  open_in_browser=True, map_image=None):
        """
        Generate and save the visualization map.

        Args:
            vectorlist: Optional list of polygons to draw (e.g., runway, airport boundary)
            output_file: Path to save the HTML map
            open_in_browser: If True, opens the map in the default web browser
            map_image: Path to the static PNG image to use as the map background

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

        # Plot points with annotations
        for (lat, lon), annotation in zip(self.points, self.annotations):
            color = self._get_point_color(annotation)
            folium.Circle(location=[lat, lon], radius=85, color=color,
                        fill=True, fill_color=color, popup=annotation).add_to(m)
        print("Points plotted.")

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
    parser.add_argument("--map-image", type=str, default="map.png",
                        help="Path to the static PNG image for the map background.")
    parser.add_argument("--map-type", type=str, default="sectional",
                        choices=["sectional", "satellite"],
                        help="Type of base map to use: 'sectional' (VFR chart) or 'satellite' (imagery)")
    parser.add_argument("--map-opacity", type=float, default=80.0,
                        help="Opacity percentage for the base map (0-100, default: 80)")
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
    visualizer.visualize(vectorlist=None) #, map_image=args.map_image)
