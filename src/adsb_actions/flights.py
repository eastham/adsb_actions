"""Storage for all Flight objects in the system, and handling
for flight updates."""
import threading
import logging
from typing import Dict
from .flight import Flight
from .location import Location
from .rules import Rules
from .bboxes import Bboxes

from .adsb_logger import Logger

logger = logging.getLogger(__name__)
#logger.level = logging.DEBUG
LOGGER = Logger()

class Flights:
    """all Flight objects in the system, indexed by flight_id"""

    def __init__(self, bboxes, ignore_unboxed_flights=True):
        self.flight_dict: Dict[str, Flight] = {}        # all flights in the system.
        self.lock: threading.Lock = threading.Lock()    # XXX may not be needed anymore...
        self.bboxes : list[Bboxes] = bboxes             # all bboxes in the system.
        self.last_checkpoint = 0                        # timestamp of last maintenance
        self.iterator_index = 0                         # support for __next__()
        # don't match flights that aren't in a bounding box
        self.ignore_unboxed_flights = ignore_unboxed_flights

    def __iter__(self):
        return self

    def __next__(self):
        if self.iterator_index >= len(self.flight_dict):
            self.iterator_index = 0
            raise StopIteration
        keys = list(self.flight_dict.keys())
        key = keys[self.iterator_index]
        self.iterator_index += 1

        return self.flight_dict[key]

    def add_location(self, loc: Location, rules: Rules) -> float:
        """
        Track an aircraft location update, update what bounding boxes it's in,
        and process rules.

        Args:
            loc: aircraft location point to store / act on
            rules: apply these rules to the new location

        Returns:
            nonzero float of epoch timestamp just added.
        """

        if not loc.tail:
            # couldn't convert ICAO code.  Try the flight name...
            loc.tail = loc.flight
        if not loc.tail or loc.tail == "N/A":
            # Fall back to hex code to avoid all unknown aircraft being indexed
            # under the same "N/A" key
            if loc.hex:
                loc.tail = loc.hex
            else:
                return loc.now

        with self.lock: # lock needed since testing can race
            flight = self.flight_dict.get(loc.tail)
            if flight is None:
                flight = Flight(loc.tail, loc.flight, loc, loc, self.bboxes)
                self.flight_dict[loc.tail] = flight
            else:
                flight.update_loc(loc)

            flight.update_inside_bboxes(self.bboxes, loc)
            rules.process_flight(flight)

        return flight.lastloc.now

    def expire_old(self, rules, last_read_time, expire_secs):
        """Delete any flights that haven't been seen in a while.
        This is important to make proximity checks efficient."""

        count = 0
        with self.lock:
            for f in list(self.flight_dict):
                flight = self.flight_dict[f]
                if last_read_time - flight.lastloc.now > expire_secs:
                    rules.do_expire(flight)
                    del self.flight_dict[f]
                    count += 1
                    logger.debug("Expired flight %s last seen at %d, now %d",
                                 flight.flight_id, flight.lastloc.now, last_read_time)

    def find_nearby_flight(self, flight2, altsep, latsep, last_read_time) -> Flight:
        """Returns maximum of one nearby flight within the given separation, 
        None if not found"""

        MIN_FRESH = 10 # seconds.  Older locations not evaluated

        for flight1 in self.flight_dict.values():
            if flight1 is flight2:
                continue
            if self.ignore_unboxed_flights and not flight2.in_any_bbox():
                continue # performance optimization
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
