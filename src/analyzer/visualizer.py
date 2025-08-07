"""Convert CSV from analyze_from_files.py to a map with points and annotations.

This script reads a CSV file from stdin, extracts latitude and longitude
coordinates, and visualizes them on a map using Folium. The map can be
saved as an HTML file and optionally opened in a web browser."""

# sample command line:
# conda activate pyproj_env
# cat 2022.csv | python3 ../visualizer.py  --map-image ./map_anno.png

import argparse
import datetime
import folium
import geopandas as gpd
from shapely.geometry import Polygon, Point
import webbrowser
import os
import sys
import csv

def visualize_points(points, annotations, vectorlist, use_imagery=False,
                     output_file="airport_map.html", open_in_browser=True,
                     map_image="map.png"):
    """
    Visualizes an airport boundary and points with annotations on an OSM map, 
    and optionally opens it in a browser.

    Args:
        points: List of (latitude, longitude) tuples for points to be plotted.
        annotations: List of strings corresponding to the annotations for each point.
        vectorlist: list of List of (latitude, longitude) tuples defining other vector annotations.
        output_file: Path to save the HTML map.
        open_in_browser: Boolean, if True, opens the map in the default web browser.
        map_image: Path to the static PNG image to use as the map background.
    """

    if not points:
        raise ValueError("Points list cannot be empty")

    if len(points) != len(annotations):
        raise ValueError(
            "Points list and annotations list must have the same length.")

    ll_lat = 40.70396699999999
    ur_lat = 40.813967
    ll_lon = -119.28410099999999
    ur_lon = -119.094101
    center_lat = (ll_lat + ur_lat) / 2
    center_lon = (ll_lon + ur_lon) / 2

    # Create a Folium map centered on the airport
    if use_imagery:
        # Use a static PNG image as the map background
        m = folium.Map(location=[center_lat, center_lon], zoom_start=13, tiles=None)

        folium.raster_layers.ImageOverlay(
            image=map_image,  # Now uses the parameter

            # Adjust the bounds to fit the image
            bounds=[[ll_lat, ll_lon], [ur_lat, ur_lon]],
            opacity=0.8
        ).add_to(m)
    else:
        m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=13,
        tiles="https://tiles.arcgis.com/tiles/ssFJjBXIUyZDrSYZ/arcgis/rest/services/VFR_Sectional/MapServer/tile/{z}/{y}/{x}",
        attr="Esri VFR Sectional"
        )

    print("Map created.")
    # Draw the airport boundary
    if vectorlist:
        for vectors in vectorlist:
            folium.Polygon(locations=vectors, color="black",
                        fill=False, tooltip="Airport Boundary").add_to(m)
    print("Boundary drawn.")

    # Plot points with annotations
    for (lat, lon), annotation in zip(points, annotations):
        if "overtake" in annotation:
            color = "blue"
        elif "tbone" in annotation:
            color = "orange"
        elif "headon" in annotation:
            color = "red"
        else:
            color = "green"
        folium.Circle(location=[lat, lon], radius=85, color=color,
                      fill=True, fill_color=color, popup=annotation).add_to(m)
    print("Points plotted.")

    # Add a legend to the map
    m.get_root().html.add_child(folium.Element(legend_html))

    # Save the map to an HTML file
    m.save(output_file)
    print(f"Map saved to {output_file}")

    if open_in_browser:
        webbrowser.open("file://" + os.path.realpath(output_file))


# Example data
airport_boundary = [
[
    (40.783385, -119.233837),
    (40.807359, -119.217774),
    (40.803149, -119.182806),
    (40.776576, -119.177278),
    (40.764366, -119.208810)
],
[
   (40.76866, -119.18491), # 23L
   (40.76164, -119.20451)  # 5R
],[
   (40.76239, -119.21264), # 23R
   (40.75596, -119.2306)   # 5L
]
]

points = [
    (40.754, -119.264),
    (40.778, -119.239),
    (40.776, -119.231),
    (40.809, -119.21),
    (40.785, -119.145),
    (40.75, -119.124),
    (40.742, -119.177),
    (40.806, -119.225),
    (40.751, -119.125),
    (40.797, -119.166),
    (40.805, -119.222),
    (40.743, -119.17),
]

annotations = [
    "point1",
    "point2",
    "point3",
    "point4",
    "point5",
    "point6",
    "point7",
    "point8",
    "point9",
    "point10",
    "point11",
    "point12",
]
legend_html = """
    <div style="
        position: fixed;
        bottom: 50px;
        left: 50px;
        width: 270px;
        height: 190px;
        background-color: white;
        border: 2px solid black;
        z-index: 1000;
        padding: 10px;
        font-size: 14px;
    ">
        <b>YEAR: <a href="2022.html">2022</a>
        <a href="2023.html">2023</a>
        <a href="2024.html">2024</a></b><br/><br/>
        <b>Click on an event for details.</b><br>
        <b>LOS event types:</b><br>
        <i style="background: blue; width: 10px; height: 10px; display: inline-block;"></i> Overtake<br>
        <i style="background: orange; width: 10px; height: 10px; display: inline-block;"></i> T-Bone<br>
        <i style="background: red; width: 10px; height: 10px; display: inline-block;"></i> Head-On<br>
        <i style="background: green; width: 10px; height: 10px; display: inline-block;"></i> Other<br>
        
    </div>
    """
static_legend_html = """
    <div style="
        position: fixed;
        bottom: 50px;
        left: 50px;
        width: 270px;
        height: 190px;
        background-color: white;
        border: 2px solid black;
        z-index: 1000;
        padding: 10px;
        font-size: 14px;
    ">
        <b>LOS criteria:</b> within .3nm laterally AND 400 feet vertically<br>
        <br/>
        <b>LOS event types:</b><br>
        <i style="background: blue; width: 10px; height: 10px; display: inline-block;"></i> Overtake<br>
        <i style="background: orange; width: 10px; height: 10px; display: inline-block;"></i> T-Bone<br>
        <i style="background: red; width: 10px; height: 10px; display: inline-block;"></i> Head-On<br>
        <i style="background: green; width: 10px; height: 10px; display: inline-block;"></i> Other<br>
    </div>
    """
POINT_LIST = []
ANNOTATION_LIST = []

def visualize_point(point, annotation):
    """Build a list for future sending to visualize_points"""
    POINT_LIST.append(point)
    ANNOTATION_LIST.append(annotation)

def finalize_visualization(vectorlist, use_imagery, map_image="map.png"):
    """Call the visualization function with the built lists"""
    visualize_points(POINT_LIST, ANNOTATION_LIST, vectorlist,
                     use_imagery=use_imagery, map_image=map_image)
    # Clear the lists after visualization
    POINT_LIST.clear()
    ANNOTATION_LIST.clear()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Visualize points on a map.")
    parser.add_argument("--map-image", type=str, default="map.png", 
                        help="Path to the static PNG image for the map background.")
    args = parser.parse_args()
    # accept on stdin a csv with rows that look like:
    # timestamp,lat,long,altidude,tail1,tail2
    csv_reader = csv.reader(sys.stdin)

    # Parse the CSV
    ctr = 0
    for row in csv_reader:
        if len(row) < 6:
            continue  # Skip rows with insufficient data
        try:
            _, timestamp, _, datestr, lat, lon, altitude, tail1, tail2, _, link, interp, audio, type, phase, *rest = row
            annotation = f"{datestr} {tail1}/{tail2} <b>Alt:</b> {altitude} <b>type:</b> {type} <b>phase:</b> {phase}"
            annotation += f" <b>narrative:</b> {interp}<br> <a href='{link}' target='_blank'>REPLAY LINK</a>"
            if len(audio) > 1: 
                annotation += f" <b>ATC audio:</b> {audio}<br>"
            visualize_point((float(lat), float(lon)), annotation)
            ctr += 1
        except ValueError as e:
            print(f"Parse error on row: {row} " + str(e) )

    print(f"Visualizing {ctr} points.")
    finalize_visualization(None, use_imagery=True, map_image=args.map_image)

    # visualize_points(points, annotations, airport_boundary, use_imagery=False)
