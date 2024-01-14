"""Storage for all Flight objects in the system, and handling
for flight updates."""
import threading
import logging
from typing import Dict
from flight import Flight
from location import Location
from rules import Rules
import bboxes

logger = logging.getLogger(__name__)

class Flights:
    """all Flight objects in the system, indexed by flight_id"""
    EXPIRE_SECS: int = 180  # 3 minutes emperically needed to debounce poor-signal airplanes

    def __init__(self, bboxes):
        self.flight_dict: Dict[str, Flight] = {}        # all flights in the system.
        self.lock: threading.Lock = threading.Lock()    # XXX may not be needed anymore...
        self.bboxes : list[bboxes.BBoxes] = bboxes      # all bboxes in the system.
        self.last_checkpoint = 0                        # timestamp of last maintenance

    def add_location(self, loc: Location, rules: Rules) -> float:
        """
        Track an aircraft location update, update what bounding boxes it's in,
        and process rules to update the gui or do user-defined tasks.

        Args:
            loc: aircraft location point to store / act on
            rules: apply these rules to the new location

        Returns:
            nonzero float of epoch timestamp just added.
        """

        if not loc.tail:
            # couldn't convert ICAO code.  Try the flight name...
            loc.tail = loc.flight
        if not loc.tail:
            return loc.now

        self.lock.acquire() # lock needed since testing can race

        if loc.tail in self.flight_dict:
            flight = self.flight_dict[loc.tail]
            flight.update_loc(loc)
        else:
            flight = self.flight_dict[loc.tail] = Flight(loc.tail, loc.flight, loc,
                                                         loc, self.bboxes)

        flight.update_inside_bboxes(self.bboxes, loc)
        #print(flight.to_str())
        rules.process_flight(flight)

        self.lock.release()
        return flight.lastloc.now

    def expire_old(self, rules, last_read_time):
        """Delete any flights that haven't been seen in a while.
        This is important to make proximity checks efficient."""

        logger.debug("Expire_old")

        with self.lock:
            for f in list(self.flight_dict):
                flight = self.flight_dict[f]
                if last_read_time - flight.lastloc.now > self.EXPIRE_SECS:
                    rules.do_expire(flight)
                    del self.flight_dict[f]

    def find_nearby_flight(self, flight2, altsep, latsep, last_read_time) -> Flight:
        """Returns maximum of one nearby flight within the given separation, 
        None if not found"""

        MIN_FRESH = 10 # seconds.  Older locations not evaluated

        for flight1 in self.flight_dict.values():
            if flight1 is flight2:
                continue
            if not flight2.in_any_bbox():
                continue # NOTE optimization, maybe not desired behavior
            if last_read_time - flight2.lastloc.now > MIN_FRESH:
                continue

            loc1 = flight1.lastloc
            loc2 = flight2.lastloc
            #logger.debug(f"dist {flight1.flight_id} and {flight2.flight_id}: {loc1-loc2}")

            if abs(loc1.alt_baro - loc2.alt_baro) < altsep:
                dist = loc1 - loc2

                if dist < latsep:
                    logger.debug("%s-%s inside minimum distance %.1f nm",
                        flight1.flight_id, flight2.flight_id, dist)
                    return flight1
        return None
