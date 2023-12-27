import threading
import logging
from typing import Dict
from flight import Flight
from location import Location
from rules import Rules

class Flights:
    """all Flight objects in the system, indexed by flight_id"""
    flight_dict: Dict[str, Flight] = {}
    lock: threading.Lock = threading.Lock()
    EXPIRE_SECS: int = 180  # 3 minutes emperically needed to debounce poor-signal airplanes

    def __init__(self, bboxes):
        self.bboxes = bboxes

    def add_location(self, loc: Location, rules: Rules):
        """
        Track an aircraft location update, update what bounding boxes it's in,
        and fire callbacks to update the gui or do user-defined tasks.

        loc: Location/flight info to update
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
            flight = self.flight_dict[flight_id] = Flight(flight_id, loc.tail, loc, loc, self.bboxes)

        flight.update_inside_bboxes(self.bboxes, loc)
        #print(flight.to_str())
        rules.process_flight(flight)

        self.lock.release()
        return flight.lastloc.now

    def expire_old(self, rules, last_read_time):
        self.lock.acquire()
        for f in list(self.flight_dict):
            flight = self.flight_dict[f]
            if last_read_time - flight.lastloc.now > self.EXPIRE_SECS:
                rules.do_expire(flight)
                del self.flight_dict[f]

        self.lock.release()

    def check_distance(self, rules, last_read_time):
        flight_list = list(self.flight_dict.values())

        rules.handle_proximity_condition(flight_list)