"""
This module provides functionality for storing and resampling location updates 
at a fixed interval.
"""

import logging
import datetime
from typing import Dict, List, Optional, Tuple
from .location import Location
from .adsb_logger import Logger
from .flights import Flights

logger = logging.getLogger(__name__)
#logger.level = logging.DEBUG
LOGGER = Logger()

EXPIRE_TIME = 60 # seconds before we expire a location report.

class Resampler:
    """Stores / resamples locations, then can check for proximity events.
    """

    def __init__(self):
        # Mapping from tail number to list of locations
        self.tailhistory: Dict[str, List[Location]] = {}
        # Mapping from timestamp to list of locations
        self.timehistory: Dict[int, List[Location]] = {}

        self.min_time: Optional[int] = None
        self.max_time: Optional[int] = None

    def add_location(self, location: Location, minalt=3000, maxalt=12000) -> None:
        """Add a location to the history, and resample for this aircraft 
        backwards in time.  It does this by looking up the previous location
        for this aircraft, then interpolating between the two locations.
        
        Args:
            location: The location to add
            minalt: don't resample below this altitude -- exclude ground contacts
            maxalt: optimization -- ignore locations above this altitude
        """
        if not location.tail:
            return  # Skip locations without a tail number

        flight_id = location.tail
        now = location.now
        if not minalt <= location.alt_baro <= maxalt:
            return

        # Add interpolated locations to the time history -- look for previous entries
        # from this tail number, and if found, fill in the gaps
        if flight_id in self.tailhistory:
            prev_locations = self.tailhistory[flight_id]
            if prev_locations:
                prev_location = prev_locations[-1]

                if prev_location.now + EXPIRE_TIME > now and now - 1 > prev_location.now + 1:
                    # Fill in the gap between the last location and the new one
                    for t in range(int(prev_location.now) + 1, int(now)):
                        if t not in self.timehistory:
                            self.timehistory[t] = []
                        interp_location = interpolate_location(
                            prev_location, location, t)
                        if interp_location:
                            self.timehistory[t].append(interp_location)

        # Add the current location to the histories
        if flight_id not in self.tailhistory:
            self.tailhistory[flight_id] = []
        self.tailhistory[flight_id].append(location)

        if now not in self.timehistory:
            self.timehistory[now] = []
        self.timehistory[now].append(location)

        # Update global time ranges
        if self.min_time is None or location.now < self.min_time:
            self.min_time = location.now
        if self.max_time is None or location.now > self.max_time:
            self.max_time = location.now

    def do_prox_checks(self, rules, bboxes, sample_interval: int = 1,
                       gc_callback = None) -> None:
        """Inspect resampled history to detect proximity events.
        
        This method:
        1. Gets the overall time range from the location history
        2. Iterates through this range at fixed intervals
        3. Builds up a Flights object to track what's active
        4. Processes proximity rules for the flights in the object, using the
           same methods aas the standard event loop.
        
        Args:
            rules: Rules object containing proximity rules
            bboxes: bboxes to limit prox checkes to, if any
            sample_interval: Time interval in seconds between samples
            gc_callback: Optional callback for finalization or garbage collection
        """

        # Get all proximity rules
        if not rules.get_rules_with_condition("proximity"):
            logger.error("No proximity rules found for resampling")
            return
        if self.min_time is None or self.max_time is None:
            logger.error("No time history available for resampling")
            return

        logger.debug("Analyzing resampled time range: %.1f to %.1f",
                     self.min_time, self.max_time)
        flights = Flights(bboxes)
        found_prox_events = []
        location_ctr = 0

        # Iterate through time range
        for current_time in range(int(self.min_time), int(self.max_time) + 1,
                                  sample_interval):
            utc_time = datetime.datetime.fromtimestamp(current_time,
                                                       datetime.UTC)
            if current_time % 1000 == 0:
                logger.info("Doing prox checks at time %s", utc_time)

            # Add locations from timehistory to flights
            if current_time in self.timehistory:
                for loc in self.timehistory[current_time]:
                    flights.add_location(loc, rules)
                    logger.debug("Adding location %s to flights at %s",
                                loc.to_str(), utc_time)
                    location_ctr += 1

            # Process proximity rules
            found = rules.handle_proximity_conditions(flights, current_time)
            if found:
                found_prox_events.append(found)

            # Clear out recently-unseen locations
            flights.expire_old(rules, current_time, EXPIRE_TIME)

            if gc_callback:
                gc_callback(current_time)

        print(f"Processed {location_ctr} resampled events.")
        return found_prox_events

def interpolate_location(loc1: Location, loc2: Location, timestamp: float) -> Optional[Location]:
    """Interpolate between two locations based on timestamp.
    
    Args:
        loc1: First location (earlier timestamp)
        loc2: Second location (later timestamp)
        timestamp: Timestamp to interpolate at
        
    Returns:
        Interpolated location or None if timestamp is outside the range
    """
    # Check if timestamp is within the range
    if timestamp < loc1.now or timestamp > loc2.now:
        return None

    # Calculate interpolation factor (0.0 to 1.0)
    if loc2.now == loc1.now:  # Avoid division by zero
        factor = 0.0
    else:
        factor = (timestamp - loc1.now) / (loc2.now - loc1.now)

    # Interpolate values
    lat = loc1.lat + factor * (loc2.lat - loc1.lat)
    lon = loc1.lon + factor * (loc2.lon - loc1.lon)
    alt_baro = int(loc1.alt_baro + factor * (loc2.alt_baro - loc1.alt_baro))

    # For track, handle the case where it wraps around 360 degrees
    track_diff = loc2.track - loc1.track
    if abs(track_diff) > 180:
        # Adjust for wrap-around
        if track_diff > 0:
            track_diff -= 360
        else:
            track_diff += 360
    track = (loc1.track + factor * track_diff) % 360

    # Interpolate ground speed
    gs = loc1.gs + factor * (loc2.gs - loc1.gs)

    # Create new location with interpolated values
    new_loc = Location(
        lat=lat,
        lon=lon,
        alt_baro=alt_baro,
        now=timestamp,
        flight=loc1.flight,
        hex=loc1.hex,
        tail=loc1.tail,
        gs=gs,
        track=track
    )
    return new_loc
