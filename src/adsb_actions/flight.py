"""Flight module represents the internal state for one flight.
Location last seen, tail number, etc."""

import datetime
import statistics
import logging
from dataclasses import dataclass, field
from threading import Lock
from .location import Location
from .bboxes import Bboxes
from .adsb_logger import Logger

logger = logging.getLogger(__name__)
#logger.level = logging.DEBUG
LOGGER = Logger()

@dataclass
class Flight:
    """Summary of a series of locations, plus other annotations"""
    # CAUTION when changing -- dataclass -- constructor positional args implied

    # This is the tail number as best as we can determine.
    # It is derived from the ICAO hex code if we can grok it, otherise
    # rely on the user-provided "flight" which might be a tail number,
    # a flight number, etc.
    # Includes country prefix "N", "C-", etc.
    # Will always be a non-zero-length string.
    flight_id: str

    # User-provided "flight" field from ADS-B.
    # Might be a tail number, flight name, or nothing / None.
    # Watch out for differences like flight_id = "N1234", other_id = "1234"
    other_id: str
    firstloc: Location  # first location we ever saw this aircraft
    lastloc: Location   # most recent location.  note includes timestamp
    all_bboxes_list: list = field(default_factory=list) # all bboxes in the system

    # variables typically not used in contstructor args below this point:
    external_id: str = None # optional, database id for this flight
    alt_list: list = field(default_factory=list)  # last n altitudes we've seen
    threadlock: Lock = field(default_factory=Lock)
    flags: dict = field(default_factory=lambda: ({}))  # persistent notes taken about this flight

    # bbox lists are indexed by kml file, in the order they were specified.
    inside_bboxes: list = field(default_factory=list)  # list of bbox names we're in
    inside_bboxes_indices: list = field(default_factory=list) # list of bbox indices
    prev_inside_bboxes = None           # what bboxes were we inside at last position update
    prev_inside_bboxes_valid = False    # true after 2nd update

    def __post_init__(self):
        assert self.flight_id and len(self.flight_id) > 0
        self.inside_bboxes = [None] * len(self.all_bboxes_list)
        self.inside_bboxes_indices = [None] * len(self.all_bboxes_list)

    def to_str(self):
        """String representation includes lat/long and bbox list."""

        string = self.lastloc.to_str()

        string += " " + str(self.inside_bboxes)
        return string

    def to_link(self):
        """Return a live-map url for this flight."""
        return f"https://globe.airplanes.live/?lat={self.lastloc.lat}&lon={self.lastloc.lon}&zoom=10"

    def to_recording(self):
        """Return a recorded url for this flight."""

        # format aircraft lastloc time like 2024-07-29-22:08
        timestamp = datetime.datetime.fromtimestamp(
            self.lastloc.now, datetime.timezone.utc).strftime("%Y-%m-%d-%H:%M")
        return f"https://globe.airplanes.live/?replay={timestamp}&lat={self.lastloc.lat}&lon={self.lastloc.lon}&zoom=10"

    def lock(self):
        self.threadlock.acquire()

    def unlock(self):
        self.threadlock.release()

    def in_any_bbox(self):
        for bbox in self.inside_bboxes:
            if bbox is not None: return True
        return False

    def was_in_any_bbox(self):
        if not self.prev_inside_bboxes_valid:
            return False
        for bbox in self.prev_inside_bboxes:
            if bbox is not None:
                return True
        return False

    def is_in_bboxes(self, bb_list: list):
        """Is the flight in any of the bboxes specified in bb_list?
        If no boxes are specified on bb_list (None, [None], []), the flight 
        must have been in no boxes to match."""

        # special case: empty bb_list requires flight to be in no boxes
        if bb_list is None or bb_list == []:
            return not self.in_any_bbox()

        for in_bb in self.inside_bboxes:
            if in_bb in bb_list:
                return True
        return False

    def was_in_bboxes(self, bb_list: list):
        """Was the flight in any of the bboxes specified in bb_list?
        if no boxes are specified on bb_list (None, [None], []), the flight 
        must have been in no boxes to match."""
        if not self.prev_inside_bboxes_valid:
            return bb_list == [None]

        if bb_list is None or bb_list == []:
            return not self.was_in_any_bbox()

        for prev_bb in self.prev_inside_bboxes:
            if prev_bb in bb_list:
                return True
        return False

    def track_alt(self, alt: int) -> int:
        """Update a running tally and average of recent altitudes.
        Returns 1 if increasing, -1 if decreasing, 0 if no change. """
        ALT_TRACK_ENTRIES = 5

        avg = alt
        if len(self.alt_list):
            avg = statistics.fmean(self.alt_list)
        if len(self.alt_list) == ALT_TRACK_ENTRIES:
            self.alt_list.pop(0)
        self.alt_list.append(alt)

        avg = int(avg)
        if alt > avg: return 1
        if alt < avg: return -1
        return 0

    def get_alt_change_str(self, alt: int) -> str:
        """Update our state with a new altitude, and return an up or down 
        arrow to reflect altitude trend"""
        altchange = self.track_alt(alt)

        altchangestr = "  "
        if altchange > 0:
            altchangestr = "^"
        if altchange < 0:
            altchangestr = "v"

        return altchangestr

    def update_loc(self, loc):
        self.lastloc = loc

    def update_inside_bboxes(self, bbox_list : list[Bboxes], loc : Location):
        """
        Based on the most recent position data, update what bounding boxes we're in.
        Note: all array indices [i] in this function are selecting between kml files.
        """
        if self.prev_inside_bboxes is not None:
            self.prev_inside_bboxes_valid = True

        self.prev_inside_bboxes = self.inside_bboxes.copy()

        for i, bbox in enumerate(bbox_list):
            self.inside_bboxes[i] = None
            self.inside_bboxes_indices[i] = None

            match_index = bbox_list[i].contains(loc.lat, loc.lon, loc.track, loc.alt_baro)

            if match_index >= 0 and self.inside_bboxes[i] != bbox_list[i].boxes[match_index].name:
                # Flight changed bounding boxes at level i
                self.inside_bboxes[i] = bbox_list[i].boxes[match_index].name
                self.inside_bboxes_indices[i] = match_index

        # logging only below this point
        if self.inside_bboxes != self.prev_inside_bboxes:
            timestamp = datetime.datetime.fromtimestamp(
                self.lastloc.now).strftime("%m/%d/%y %H:%M")
            logger.debug("%s Bbox change for %s: was %s now %s %s", timestamp,
                        self.flight_id, self.prev_inside_bboxes, self.inside_bboxes, loc.to_str())

    def get_bbox_at_level(self, level) -> str:
        """return the bbox name that we're in for the given kml file."""
        return self.inside_bboxes[level]
