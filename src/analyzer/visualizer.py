import datetime
import folium
import geopandas as gpd
from shapely.geometry import Polygon, Point
import webbrowser
import os
import sys
import csv

# conda activate pyproj_env

def visualize_points(points, annotations, vectorlist, use_imagery=False,
                     output_file="airport_map.html", open_in_browser=True,
                     center=None):
    """
    Visualizes an airport boundary and points with annotations on an OSM map, 
    and optionally opens it in a browser.

    Args:
        points: List of (latitude, longitude) tuples for points to be plotted.
        annotations: List of strings corresponding to the annotations for each point.
        vectorlist: list of List of (latitude, longitude) tuples defining other vector annotations.
        output_file: Path to save the HTML map.
        open_in_browser: Boolean, if True, opens the map in the default web browser.
    """

    if not points:
        raise ValueError("Points list cannot be empty")

    if len(points) != len(annotations):
        raise ValueError(
            "Points list and annotations list must have the same length.")

    # Calculate the center of the airport boundary for map centering
    airport_boundary = vectorlist[0]
    if center:
        center_lat, center_lon = center
    else:
        center_lat = sum(lat for lat, lon in airport_boundary) / \
            len(airport_boundary)
        center_lon = sum(lon for lat, lon in airport_boundary) / \
            len(airport_boundary)

    print("Center Latitude:", center_lat)
    print("Center Longitude:", center_lon)

    # Create a Folium map centered on the airport
    if use_imagery:
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=12,
            tiles="https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
            attr="Esri World Imagery",  # You can change this to a VFR tile source.
        )
    else:
        m = folium.Map(location=[center_lat, center_lon], zoom_start=12)

    print("Map created.")
    # Draw the airport boundary
    for vectors in vectorlist:
        folium.Polygon(locations=vectors, color="black",
                    fill=False, tooltip="Airport Boundary").add_to(m)
    print("Boundary drawn.")

    # Plot points with annotations
    for (lat, lon), annotation in zip(points, annotations):
        folium.Circle(location=[lat, lon], radius=85, color="red",
                      fill=True, fill_color="red", popup=annotation).add_to(m)
    print("Points plotted.")

    # Save the map to an HTML file
    m.save(output_file)
    print(f"Map saved to {output_file}")

    if open_in_browser:
        webbrowser.open("file://" + os.path.realpath(output_file))


# Example data
airport_boundary = [[
    (40.783385, -119.233837),
    (40.807359, -119.217774),
    (40.803149, -119.182806),
    (40.776576, -119.177278),
    (40.764366, -119.208810)
],[
    (40.76866, -119.18491), # 23L
    (40.76164, -119.20451)  # 5R
],[
    (40.76239, -119.21264), # 23R
    (40.75596, -119.2306)   # 5L
]]

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

POINT_LIST = []
ANNOTATION_LIST = []

def visualize_point(point, annotation):
    """Build a list for future sending to visualize_points"""
    POINT_LIST.append(point)
    ANNOTATION_LIST.append(annotation)

def finalize_visualization(vectorlist, use_imagery):
    """Call the visualization function with the built lists"""
    visualize_points(POINT_LIST, ANNOTATION_LIST, vectorlist,
                     use_imagery=use_imagery)
    # Clear the lists after visualization
    POINT_LIST.clear()
    ANNOTATION_LIST.clear()

if __name__ == "__main__":
    # accept on stdin a csv with rows that look like:
    # timestamp,lat,long,altidude,tail1,tail2
    csv_reader = csv.reader(sys.stdin)

    # Parse the CSV
    ctr = 0
    for row in csv_reader:
        if len(row) < 6:
            continue  # Skip rows with insufficient data
        try:
            timestamp, lat, lon, altitude, tail1, tail2 = row
            ts_int = int(timestamp.split(" ")[-1])
            ts_gmt = datetime.datetime.utcfromtimestamp(ts_int).strftime("%m/%d/%y %H:%M:%S")
            annotation = f"{ts_gmt}, Alt: {altitude}, Tail: {tail1}/{tail2}"
            annotation += f" ({lat}, {lon})"
            visualize_point((lat, lon), annotation)
            ctr += 1
        except ValueError:
            print(f"Parse error on row: {row}")

    print(f"Visualizing {ctr} points.")
    finalize_visualization(airport_boundary, use_imagery=True)

    # visualize_points(points, annotations, airport_boundary, use_imagery=False)
