"""Representation for bounding boxes found in kml files.
These must be specified in the KML as a Polygon with a name of the form:
label: minalt-maxalt minhdg-maxhdg
For example:
Rwy 25 Approach: 4500-5500 230-270

Note there is a Bbox object and a Bboxes object, the latter containing Bbox objects."""

import re
import warnings
import logging

from dataclasses import dataclass
from typing import List, Tuple
# for fastkml, which breaks in newer versions
warnings.filterwarnings("ignore", category=DeprecationWarning)
from fastkml import kml, features, containers
from .adsb_logger import Logger

logger = logging.getLogger(__name__)
#logger.level = adsb_logger.logging.DEBUG
LOGGER = Logger()

@dataclass
class Bbox:
    """A single bounding box defined by a polygon, altitude range, and heading range."""
    polygon_coords: List[Tuple[float, float]]  # List of (x, y) coordinates
    minalt: int
    maxalt: int
    starthdg: int
    endhdg: int
    name: str

class Bboxes:
    """
    A collection of Bbox objects, defined by a KML file with polygons inside.
    Each polygon should have a name formatted like this in the KML: 
        name: minalt-maxalt minhdg-maxhdg
    For example:
        RHV apporach: 500-1500 280-320
    """
    def __init__(self, fn):
        self.boxes: list[Bbox] = []

        try:
            k = kml.KML.parse(fn, strict=False)
            kml_features = list(k.features)
        except Exception as e:
            logger.error("Error parsing KML file %s: %s", fn, str(e))
            raise ValueError("KML parse error: " + str(e)) from e

        self.parse_placemarks(kml_features)
        if len(self.boxes) == 0:
            logger.warning("No bboxes found")
        logger.info("Setup done for bboxes in %s", fn)

    def parse_placemarks(self, document):
        """Parses a placemark of the form:
        Name: minalt-maxalt minheading-maxheading

        for example, this defines a region called "Rwy 25 Approach" from
        4500-5500 feet, with heading 230 to 270:

        Rwy 25 Approach: 4500-5500 230-270
        """
        for feature in document:
            if isinstance(feature, features.Placemark):
                re_result = re.search(r"^([^:]+):\s*(-?\d+)-(-?\d+)\s+(\d+)-(\d+)",
                    feature.name)
                if not re_result:
                    raise ValueError("KML feature name parse error: " +
                        feature.name)
                name = re_result.group(1)
                minalt = int(re_result.group(2))
                maxalt = int(re_result.group(3))
                starthdg = int(re_result.group(4))
                endhdg = int(re_result.group(5))

                logger.debug("Adding bounding box %s: %d-%d %d-%d deg",
                    name, minalt, maxalt, starthdg, endhdg)

                coords = list(feature.kml_geometry.geometry.exterior.coords)
                newbox = Bbox(polygon_coords=coords,
                    minalt=minalt, maxalt=maxalt, starthdg=starthdg,
                    endhdg=endhdg, name=name)
                self.boxes.append(newbox)

        for feature in document:
            # Note: recursive calls, some systems put features in invisible folders...
            if isinstance(feature, containers.Folder):
                self.parse_placemarks(list(feature.features))
            if isinstance(feature, containers.Document):
                self.parse_placemarks(list(feature.features))

    def contains(self, lat, long, hdg, alt):
        """returns index of first matching bounding box, or -1 if not found"""
        for i, box in enumerate(self.boxes):
            if (point_in_polygon(long, lat, box.polygon_coords) and
                Bboxes.hdg_contains(hdg, box.starthdg, box.endhdg)):
                if (alt >= box.minalt and alt <= box.maxalt):
                    return i
        return -1

    @classmethod
    def hdg_contains(cls, hdg, start, end):
        """Is the given heading within the start and end?"""
        try:
            if end < start:
                return hdg >= start or hdg <= end
            return hdg >= start and hdg <= end
        except (TypeError, ArithmeticError):
            logger.critical("Math error in heading check")
            return False

def point_in_polygon(x, y, polygon_coords):
    """Ray-casting algorithm for point-in-polygon test.

    Args:
        x: X coordinate (longitude) of the point
        y: Y coordinate (latitude) of the point
        polygon_coords: List of (x, y) tuples defining the polygon vertices

    Returns:
        True if point is inside the polygon, False otherwise
    """
    n = len(polygon_coords)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = polygon_coords[i][:2]  # Handle coords with optional z value
        xj, yj = polygon_coords[j][:2]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside
