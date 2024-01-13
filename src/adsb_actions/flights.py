"""Storage for all Flight objects in the system, and handling
for flight updates."""
import threading
import logging
from typing import Dict
from flight import Flight
from location import Location
from rules import Rules

logger = logging.getLogger(__name__)

class Flights:
    """all Flight objects in the system, indexed by flight_id"""
    EXPIRE_SECS: int = 180  # 3 minutes emperically needed to debounce poor-signal airplanes

    def __init__(self, bboxes):
        self.flight_dict: Dict[str, Flight] = {}
        self.lock: threading.Lock = threading.Lock()
        self.bboxes = bboxes
        self.last_checkpoint = 0

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

        flight_id = loc.flight
        # XXX do we always convert from icao?  have seen some aircraft with
        # empty string for flight_id
        if not flight_id or flight_id == "N/A": return loc.now

        self.lock.acquire() # lock needed since testing can race

        if flight_id in self.flight_dict:
            is_new_flight = False
            flight = self.flight_dict[flight_id]
            flight.update_loc(loc)
        else:
            is_new_flight = True
            flight = self.flight_dict[flight_id] = Flight(flight_id, loc.tail, loc,
                                                          loc, self.bboxes)

        flight.update_inside_bboxes(self.bboxes, loc)
        #print(flight.to_str())
        rules.process_flight(flight)

        self.lock.release()
        return flight.lastloc.now

    def expire_old(self, rules, last_read_time):
        logger.debug("Expire_old")
        self.lock.acquire()
        for f in list(self.flight_dict):
            flight = self.flight_dict[f]
            if last_read_time - flight.lastloc.now > self.EXPIRE_SECS:
                rules.do_expire(flight)
                del self.flight_dict[f]

        self.lock.release()

    def find_nearby_flight(self, flight2, altsep, latsep, last_read_time):
        """returns a nearby flight within the given separation, None if not found"""
        MIN_FRESH = 10 # seconds.  Older data not evaluated

        for flight1 in self.flight_dict.values():
            if flight1 is flight2:
                continue
            if not flight2.in_any_bbox():
                continue # XXX optimization, maybe not desired behavior
            if last_read_time - flight2.lastloc.now > MIN_FRESH:
                continue

            loc1 = flight1.lastloc
            loc2 = flight2.lastloc
            #if (loc1.alt_baro < MIN_ALT or loc2.alt_baro < MIN_ALT): continue
            if abs(loc1.alt_baro - loc2.alt_baro) < altsep:
                dist = loc1 - loc2

                if dist < latsep:
                    print("%s-%s inside minimum distance %.1f nm" %
                        (flight1.flight_id, flight2.flight_id, dist))
                    print("LAT, %f, %f, %d" % (flight1.lastloc.lat, 
                                               flight1.lastloc.lon, last_read_time))
                    return flight1
        return None
