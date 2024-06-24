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
from shapely.geometry import Point, Polygon
# for fastkml, which breaks in newer versions
warnings.filterwarnings("ignore", category=DeprecationWarning)
from fastkml import kml

from .adsb_logger import Logger

logger = logging.getLogger(__name__)
#logger.level = adsb_logger.logging.DEBUG
LOGGER = Logger()

@dataclass
class Bbox:
    """A single bounding box defined by a polygon, altitude range, and heading range."""
    polygon: Polygon
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

        with open(fn, 'rt', encoding="utf-8") as myfile:
            doc = myfile.read()
        k = kml.KML()
        k.from_string(doc.encode('utf-8'))
        features = list(k.features())
        self.parse_placemarks(features)
        logger.info("Setup done")

    def parse_placemarks(self, document):
        """Parses a placemark of the form:
        Name: minalt-maxalt minheading-maxheading

        for example, this defines a region called "Rwy 25 Approach" from 
        4500-5500 feet, with heading 230 to 270:

        Rwy 25 Approach: 4500-5500 230-270
        """
        for feature in document:
            if isinstance(feature, kml.Placemark):
                re_result = re.search(r"^([^:]+):\s*(\d+)-(\d+)\s+(\d+)-(\d+)",
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

                newbox = Bbox(polygon=Polygon(feature.geometry),
                    minalt=minalt, maxalt=maxalt, starthdg=starthdg,
                    endhdg=endhdg, name=name)
                self.boxes.append(newbox)

        for feature in document:
            # Note: recursive calls, some systems put features in invisible folders...
            if isinstance(feature, kml.Folder):
                self.parse_placemarks(list(feature.features()))
            if isinstance(feature, kml.Document):
                self.parse_placemarks(list(feature.features()))

    def contains(self, lat, long, hdg, alt):
        """returns index of first matching bounding box, or -1 if not found"""
        for i, box in enumerate(self.boxes):
            if (box.polygon.contains(Point(long,lat)) and
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
