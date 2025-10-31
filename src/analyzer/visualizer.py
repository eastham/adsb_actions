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

# Corners of the map
LL_LAT = 40.7145599
UR_LAT = 40.8181649
LL_LON = -119.2769929
UR_LON = -119.0903859

def visualize_points(points, annotations, vectorlist,
                     output_file="airport_map.html", open_in_browser=True,
                     map_image="map.png"):
    """
    Visualizes an airport boundary and points with annotations on an OSM map, 
    and optionally opens it in a browser.

    Args:
        points: List of (latitude, longitude) tuples for points to be plotted.
        annotations: List of strings for annotating each point in points.
        vectorlist: optional polygon to draw on the map (e.g. runway, airport boundary)
        output_file: Path to save the HTML map.
        open_in_browser: If True, opens the map in the default web browser.
        map_image: Path to the static PNG image to use as the map background.
    """

    if not points:
        raise ValueError("Points list cannot be empty")

    if len(points) != len(annotations):
        raise ValueError(
            "Points list and annotations list must have the same length.")

    center_lat = (LL_LAT + UR_LAT) / 2
    center_lon = (LL_LON + UR_LON) / 2

    # Create a Folium map centered on the airport
    if map_image:
        # Use a static PNG image as the map background
        m = folium.Map(location=[center_lat, center_lon], zoom_start=13, tiles=None)

        folium.raster_layers.ImageOverlay(
            image=map_image,
            # Adjust the bounds to fit the image
            bounds=[[LL_LAT, LL_LON], [UR_LAT, UR_LON]],
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

def finalize_visualization(vectorlist, map_image="map.png"):
    """Call the visualization function with the built lists"""
    visualize_points(POINT_LIST, ANNOTATION_LIST, vectorlist,
                     map_image=map_image)
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
            #timestamp, _, datestr, lat, lon, altitude, tail1, tail2, _, _, distance, altsep, link, interp, audio, type, phase = row
            timestamp, _, datestr, lat, lon, altitude, tail1, tail2, _, link, interp, audio, type, phase, _, distance, altsep, = row

            annotation = f"{datestr} {tail1}/{tail2} <b>Alt:</b> {altitude} <b>type:</b> {type} <b>phase:</b> {phase}"
            annotation += f" <b>Interp:</b> {interp} <b><a href='{link}' target='_blank'>REPLAY LINK</a></b> <b>ATC audio:</b> {audio}<br>"
            visualize_point((float(lat), float(lon)), annotation)
            print(f"Read point {ctr} at {lat} {lon} alt: {altitude} datestr:{datestr}")
            ctr += 1
        except ValueError as e:
            print(f"Parse error on row: {row} " + str(e) )

    print(f"Visualizing {ctr} points.")
    finalize_visualization(None, map_image=args.map_image)
