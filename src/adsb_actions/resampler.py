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
logger.level = logging.INFO
LOGGER = Logger()

MAX_INTERPOLATE_SECS = 60 # max seconds that we'll interpolate over.
MIN_ALTITUDE = 3000  # optimization: minimum altitude for resampling
MAX_ALTITUDE = 12000  # optimization: maximum altitude for resampling

class Resampler:
    """Stores / resamples locations, then can check for proximity events.
    """

    def __init__(self):
        # Mapping from flight_id to a list of location samples.
        # Resampled locations are not added here.
        self.locations_by_flight_id: Dict[str, List[Location]] = {}

        # Mapping from timestamp to list of locations.  Resampled and real locations
        # are combined here.
        # Note: Keys are int (1-second granularity).  Multiple locations within 
        # the same second will be stored in the same list.
        self.locations_by_time: Dict[int, List[Location]] = {}

        # These help rename each flight with a sequence number: flight_id_N,
        # where N is the Nth contiguous track from that flight_id.  This is 
        # not strictly needed in this code but is sometimes useful for other
        # downstream analysis.
        self.flight_counters: Dict[str, int] = {} # tail_number -> flight count
        self.last_time_seen: Dict[str, int] = {} # tail_number -> last seen timestamp

        # Just for stats/logging:
        self.resample_ctr = 0

    def add_location(self, location: Location) -> None:
        """Add a location to the history, and resample for this aircraft
        backwards in time.  It does this by looking up the previous location
        for this aircraft, then interpolating between the two locations.

        Args:
            location: The location to add
        """
        if not location.tail:
            return  # Skip locations without a tail number

        tail = location.tail
        now = location.now
        if not MIN_ALTITUDE <= location.alt_baro <= MAX_ALTITUDE:
            return

        # --- Assign unique flight_id per flight per tail ---
        # If this is the first time seeing this tail, start counter at 1
        if tail not in self.flight_counters:
            logger.info("New tail %s seen at %s", tail, datetime.datetime.fromtimestamp(now))
            self.flight_counters[tail] = 1
            self.last_time_seen[tail] = now
        else:
            # If time gap is large (EXPIRE_TIME), increment flight counter
            if now - self.last_time_seen[tail] > MAX_INTERPOLATE_SECS:
                self.flight_counters[tail] += 1
            self.last_time_seen[tail] = now

        # Assign the flight_id as tail + "_" + flight number
        flight_id = f"{tail}_{self.flight_counters[tail]}"
        location.flight = flight_id  # Adding field on the Location object

        # Add interpolated locations to the time history -- look for previous entries
        # from this flight_id, and if found, fill in the gaps
        if flight_id in self.locations_by_flight_id:
            prev_locations = self.locations_by_flight_id[flight_id]
            if prev_locations:
                prev_location = prev_locations[-1]

                # Only interpolate if:
                # 1. Gap is less than EXPIRE_TIME (don't interpolate across large gaps)
                # 2. Gap is greater than 1 second (need at least 2+ second gap to interpolate)
                time_gap = now - prev_location.now
                if time_gap <= MAX_INTERPOLATE_SECS and time_gap > 1:
                    # Fill in the gap between the last location and the new one
                    for t in range(int(prev_location.now) + 1, int(now)):
                        if t not in self.locations_by_time:
                            self.locations_by_time[t] = []
                        interp_location = interpolate_location(
                            prev_location, location, t)
                        if interp_location:
                            self.locations_by_time[t].append(interp_location)
                            self.resample_ctr += 1
                            #if "FEMG_2" in flight_id:
                            #    logger.debug("Interpolated location at %s: %s ts %d",
                            #    interp_location.to_str(), flight_id, t)
                            #logger.debug("Prev location was %s",
                            #             prev_location.to_str())
        
        # Add the current (real, not resampled) location to the histories
        if flight_id not in self.locations_by_flight_id:
            self.locations_by_flight_id[flight_id] = []
        self.locations_by_flight_id[flight_id].append(location)

        # NOTE: In the case of sub-second overlaps, all are currently stored.
        int_now = int(now)
        if int_now not in self.locations_by_time:
            self.locations_by_time[int_now] = []
        self.locations_by_time[int_now].append(location)

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
        if not self.locations_by_time:
            logger.error("No time history available for resampling")
            return

        sorted_times = sorted(self.locations_by_time.keys())
        min_time = sorted_times[0]
        max_time = sorted_times[-1]

        logger.debug("Analyzing resampled time range: %.1f to %.1f",
                     min_time, max_time)
        flights = Flights(bboxes)
        found_prox_events = []
        location_ctr = 0

        # Iterate through time range using only existing timestamps
        for current_time in sorted_times:
            if (current_time - min_time) % sample_interval != 0:
                continue

            utc_time = datetime.datetime.fromtimestamp(current_time,
                                                       datetime.UTC)
            if current_time % 1000 == 0:
                logger.info("Doing prox checks at time %s", utc_time)

            # Build up the correct system state to detect proximity at current_time.
            # Allows re-use of existing proximity handling code.  Will result in
            # callbacks being fired if conditions are met.
            for loc in self.locations_by_time[current_time]:
                flights.add_location(loc, rules)
                logger.debug("Adding location %s to flights at %s",
                            loc.to_str(), utc_time)
                location_ctr += 1

            # Process proximity rules
            found = rules.handle_proximity_conditions(flights, current_time)
            if found:
                found_prox_events.append(found)

            # Clear out recently-unseen locations
            flights.expire_old(rules, current_time, MAX_INTERPOLATE_SECS)

            if gc_callback:
                gc_callback(current_time)

        print(f"Processed {location_ctr} resampled events.")
        return found_prox_events

    def report_resampling_stats(self):
        for tail, locations in self.locations_by_flight_id.items():
            logger.info("Tail %s started with %d locations", tail, len(locations))
        logger.info("Resampling counter: %d", self.resample_ctr)
        logger.info("Timehistory locations after resampling: %d", sum(len(loc)
                    for loc in self.locations_by_time.values()))

    def for_each_resampled_point(self, callback):
        """
        Iterate through all actual and resampled points and call the 
        provided callback function with (lat, lon, alt_baro, tail_number) 
        for each point.

        Args:
            callback: A function accepting (lat, lon, alt_baro, tail_number)
        """
        callback_count = 0
        flight_ctrs = {}
        # iterate in sorted order by time
        for time in sorted(self.locations_by_time.keys()):
            locations = self.locations_by_time[time]
            for loc in locations:
                tail = loc.flight # XXX naming
                flight_ctrs[tail] = flight_ctrs.get(tail, 0) + 1
                callback(loc.lat, loc.lon, loc.alt_baro, loc.now, tail)
                callback_count += 1
                if callback_count % 10000 == 0:
                    logger.info("Processed %d callbacks so far.", callback_count)
            

        for flight_id, count in flight_ctrs.items():
            logger.info("for_each_resampled_point: flight %s saw %d total points", flight_id, count)

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
