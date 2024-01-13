"""Flight module represents the internal state for a flight.
Location, last seen time, and internal state."""

import statistics
import datetime
import logging
from dataclasses import dataclass, field
from threading import Lock
from location import Location
from stats import Stats

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG

@dataclass
class Flight:
    """Summary of a series of locations, plus other annotations"""
    # Caution when changing: ctor positional args implied
    flight_id: str  # n number if not flight id
    tail: str       # can be none
    firstloc: Location
    lastloc: Location
    all_bboxes_list: list = field(default_factory=list) # all bboxes in the system, unclear if this is needed
    external_id: str = None # optional, database id for this flight
    alt_list: list = field(default_factory=list)  # last n altitudes we've seen
    inside_bboxes: list = field(default_factory=list)  # list of bbox names, ordered by kml file
    inside_bboxes_indices: list = field(default_factory=list)  # list of bbox indices, ordered by kml file
    threadlock: Lock = field(default_factory=Lock)
    flags: dict = field(default_factory=lambda: ({}))
    prev_inside_bboxes = None
    ALT_TRACK_ENTRIES = 5

    def __post_init__(self):
        self.inside_bboxes = [None] * len(self.all_bboxes_list)
        self.inside_bboxes_indices = [None] * len(self.all_bboxes_list)

    def to_str(self):
        """
        String representation includes lat/long and bbox list
        """
        string = self.lastloc.to_str()
        bbox_name_list = []

        string += " " + str(self.inside_bboxes)
        return string

    def lock(self):
        self.threadlock.acquire()

    def unlock(self):
        self.threadlock.release()

    def in_any_bbox(self):
        for bbox in self.inside_bboxes:
            if bbox is not None: return True
        return False

    def is_in_bboxes(self, bb_list: list):
        """Is the flight in all the same bboxes as specified in list?
        Also returns true in the all-are-None condition."""

        # flight may be in [None,None], we still want to match that case
        if bb_list is None or bb_list == []:
            bb_list = [None]

        for in_bb in self.inside_bboxes:
            if in_bb in bb_list:
                return True
        return False

    def was_in_bboxes(self, bb_list: list):
        for prev_bb in self.prev_inside_bboxes:
            if prev_bb in bb_list:
                return True
        return False

    def track_alt(self, alt):
        avg = alt
        if len(self.alt_list):
            avg = statistics.fmean(self.alt_list)
        if len(self.alt_list) == self.ALT_TRACK_ENTRIES:
            self.alt_list.pop(0)
        self.alt_list.append(alt)

        avg = int(avg)
        if alt > avg: return 1
        if alt < avg: return -1
        return 0

    def get_alt_change_str(self, alt):
        altchange = self.track_alt(alt)
        altchangestr = "  "
        if altchange > 0:
            altchangestr = "^"
        if altchange < 0:
            altchangestr = "v"
        return altchangestr

    def update_loc(self, loc):
        self.lastloc = loc

    def update_inside_bboxes(self, bbox_list, loc):
        """
        Based on the most recent position data, update what bounding boxes we're in.
        Note: all array indices [i] in this function are selecting between kml files.
        """
        self.prev_inside_bboxes = self.inside_bboxes.copy()
        old_str = self.to_str()
        logger.debug("update_inside_bboxes: pre-bbox update: %s %s", self.flight_id, str(self.inside_bboxes))
        for i, bbox in enumerate(bbox_list):
            self.inside_bboxes[i] = None
            self.inside_bboxes_indices[i] = None
            match_index = bbox_list[i].contains(loc.lat, loc.lon, loc.track, loc.alt_baro)
            if match_index >= 0 and self.inside_bboxes[i] != bbox_list[i].boxes[match_index].name:
                # Flight changed bounding boxes at level i
                self.inside_bboxes[i] = bbox_list[i].boxes[match_index].name
                self.inside_bboxes_indices[i] = match_index

        if logger.level <= logging.DEBUG and self.inside_bboxes != self.prev_inside_bboxes:
            flighttime = datetime.datetime.fromtimestamp(self.lastloc.now)
            tail = self.tail if self.tail else "(unk)"
            logger.debug(tail + " Flight bbox change at " + flighttime.strftime("%H:%M") +
                ": " + self.to_str())
        else:
            logger.debug("no change to bboxes")

    def get_bbox_at_level(self, level):
        return self.inside_bboxes[level]
