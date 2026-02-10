"""Push Loss of Separation (LOS) Events to the server.
These are pushed once upon first detection and again once
expired, so that the minimum distance is logged."""

import copy
import logging
import os
import threading
import time
import datetime

from applications.airport_monitor.db_ops import add_los, update_los
from adsb_actions.stats import Stats
from adsb_actions.location import Location
from adsb_actions.adsb_logger import Logger
from adsb_actions.flight import PLAYBACK_WEBSITE

logger = logging.getLogger(__name__)
#logger.level = logging.DEBUG
LOGGER = Logger()

# Default output directory for generated animations
ANIMATION_OUTPUT_DIR = os.path.join(os.path.dirname(__file__),
                                     "../../../examples/generated")

class LOS:
    """
    Track LOS (Loss of Separation) events.  These are pushed to the server
    when initially seen, updated locally when additional callbacks come in,
    and re-pushed to the server with the final stats once the event is gc'ed.
    """
    current_los_events = {}
    finalized_los_events = {}  # Stores finalized events for post-processing
    current_los_lock: threading.Lock = threading.Lock()
    LOS_GC_TIME = 60        # seconds to wait before finalizing LOS
    LOS_GC_LOOP_DELAY = 1   # seconds between GC checks
    gc_thread = None
    quit = False
    animator = None  # Set by caller to enable animation generation
    animation_output_dir = ANIMATION_OUTPUT_DIR

    def __init__(self, flight1, flight2, latdist, altdist, create_time):
        # Keep flight1/flight2 in a universal order to enforce lock ordering
        # and consistent keys
        if flight1.flight_id > flight2.flight_id:
            self.flight2 = flight1
            self.flight1 = flight2
        else:
            self.flight1 = flight1
            self.flight2 = flight2

        # Make a deep copy of the current location to remember the location
        # of the event
        self.first_loc_1 = copy.deepcopy(flight1.lastloc)
        self.first_loc_2 = copy.deepcopy(flight2.lastloc)

        # Closest-approach distances.  Perhaps this is better represented
        # with an absolute distance?
        self.latdist = self.min_latdist = latdist
        self.altdist = self.min_altdist = altdist

        self.create_time = self.last_time = create_time
        self.cpa_time = create_time
        self.id = None

    def update(self, latdist, altdist, last_time, flight1, flight2,
               update_loc_at_closest_approach=True):
        self.latdist = latdist
        self.altdist = altdist
        self.last_time = last_time

        if latdist < self.min_latdist or altdist < self.min_altdist:
            logger.info("LOS update: new minimum for %s vs %s: %.2f nm, %d MSL at %s",
                         flight1.flight_id, flight2.flight_id, latdist, altdist,
                         datetime.datetime.utcfromtimestamp(last_time)) 
            self.min_latdist = latdist
            self.min_altdist = altdist
            self.cpa_time = last_time
            if update_loc_at_closest_approach:
                self.first_loc_1 = copy.deepcopy(flight1.lastloc)
                self.first_loc_2 = copy.deepcopy(flight2.lastloc)

    def get_key(self):
        key = "%s %s" % (self.flight1.flight_id.strip(),
            self.flight2.flight_id.strip())
        return key

def process_los_launch(flight1, flight2, do_threading=True):
    """Saw an LOS event -- in streaming mode, start a thread to 
    keep an eye on it as the event progresses.

    Args:
        do_threading: If True, process in background thread and start GC thread
            that uses wall-clock time (for real-time analysis only).
            If False, process synchronously; caller must call los_gc(timestamp)
            periodically with simulation timestamps (for offline analysis).
    """
    if do_threading:
        t = threading.Thread(target=process_los, args=[flight1, flight2])
        t.start()

        if not LOS.gc_thread:
            LOS.gc_thread = threading.Thread(target=gc_loop)
            LOS.gc_thread.start()
    else:
        process_los(flight1, flight2)

def process_los(flight1, flight2):
    """Handle a single LOS event.  Could be new, or just an update to one that's
    already underway.  Push to external database if new."""

    # Check if either flight's data is stale relative to the other.
    # If timestamps differ significantly, one aircraft stopped reporting
    # and we shouldn't trust the distance calculation.
    MIN_FRESH = 10  # seconds - must match flights.py
    now1 = flight1.lastloc.now
    now2 = flight2.lastloc.now
    if abs(now1 - now2) > MIN_FRESH:
        logger.debug("process_los skipped: timestamps too far apart (%s: %.0f, %s: %.0f)",
                     flight1.flight_id, now1, flight2.flight_id, now2)
        return

    lateral_distance = flight1.lastloc - flight2.lastloc
    alt_distance = abs(flight1.lastloc.alt_baro - flight2.lastloc.alt_baro)
    logger.info("process_los %s %s lateral dist %.2fnm %d MSL",
                flight1.flight_id, flight2.flight_id, lateral_distance, alt_distance)

    # Use the more recent timestamp as "now" for the LOS event
    now = max(now1, now2)
    # always create a new LOS at least to get flight1/flight2 ordering right
    los = LOS(flight1, flight2, lateral_distance, alt_distance, now)

    with LOS.current_los_lock:
        key = los.get_key()
        if key in LOS.current_los_events:
            logger.debug("LOS update of key %s", key)
            LOS.current_los_events[key].update(lateral_distance, alt_distance, now,
                                         flight1, flight2)
            Stats.los_update += 1
        else:
            logger.debug("LOS add key "+ key +" at " +
                         str(datetime.datetime.utcfromtimestamp(now)) +
                         ": " + flight1.to_str() + " " + flight2.to_str())
            LOS.current_los_events[key] = los
            Stats.los_add += 1

            los.id = add_los(flight1, flight2, lateral_distance,
                               alt_distance)

def log_csv_record(flight1, flight2, los, datestring, altdatestring,
                   animation_path=None):
    """Put a CSV record in the log, with replay link for post-processing.

    Args:
        flight1, flight2: Flight objects
        los: LOS object with event details
        datestring: Human-readable date string
        altdatestring: Alternate date format for replay link
        animation_path: Optional path to generated animation HTML file
    """
    meanloc = Location.meanloc(los.first_loc_1, los.first_loc_2)
    replay_time = datetime.datetime.utcfromtimestamp(
        los.create_time  # Use event start time, not end time
    ).strftime("%Y-%m-%d-%H:%M")
    link = (
        f"{PLAYBACK_WEBSITE}/"
        f"?replay={replay_time}&lat={meanloc.lat}&lon={meanloc.lon}"
        f"&zoom=12'"
    )
    animation_field = os.path.basename(animation_path) if animation_path else ""
    csv_line = (
        f"CSV OUTPUT FOR POSTPROCESSING: {los.first_loc_1.now},"
        f"{datestring},{altdatestring},{meanloc.lat},{meanloc.lon},"
        f"{meanloc.alt_baro},{flight1.flight_id.strip()},"
        f"{flight2.flight_id.strip()},notused,"
        f"{link},{animation_field},interp,audio,type,phase,,{los.min_latdist},{los.min_altdist},"
    )

    logger.info(csv_line)
    logger.info("LOS visualization: %s", animation_field if animation_field else link)


def gc_loop():
    """Run in a separate thread to periodically check for LOS events.

    NOTE: This uses wall-clock time, so it only works for real-time analysis.
    For offline/historical analysis, use do_threading=False and pass los_gc
    as a callback to do_resampled_prox_checks(), which will call it with
    simulation timestamps.
    """
    while True:
        time.sleep(LOS.LOS_GC_LOOP_DELAY)
        los_gc(time.time())
        if LOS.quit:
            return


def _generate_animation(los):
    """Generate an animation HTML file for an LOS event.

    Args:
        los: LOS object with flight1, flight2, create_time

    Returns:
        Path to the generated HTML file, or None if generation failed
    """
    if not LOS.animator:
        return None

    # Ensure output directory exists
    os.makedirs(LOS.animation_output_dir, exist_ok=True)

    # Generate filename based on tails and timestamp
    tail1 = los.flight1.flight_id.strip()
    tail2 = los.flight2.flight_id.strip()
    timestamp = datetime.datetime.utcfromtimestamp(los.create_time)
    filename = f"los_{tail1}_{tail2}_{timestamp.strftime('%Y%m%d_%H%M%S')}.html"
    output_path = os.path.join(LOS.animation_output_dir, filename)

    try:
        result = LOS.animator.animate_from_los_object(los, output_file=output_path)
        if result:
            return result
    except Exception as e:
        logger.error("Failed to generate animation for %s vs %s: %s",
                    tail1, tail2, e)

    return None

def los_gc(ts):
    """Check if any LOS events are ready to be finalized (i.e. final stats recorded)"""

    with LOS.current_los_lock:
        los_list = list(LOS.current_los_events.values())

    for los in los_list:
        logger.debug(f"LOS_GC {los.get_key()} {ts} {los.last_time}")
        flight1 = los.flight1
        flight2 = los.flight2

        if ts - los.last_time > LOS.LOS_GC_TIME:
            # No updates to this LOS for a while, finalize to database and remove.
            datestring = datetime.datetime.utcfromtimestamp(los.cpa_time)
            altdatestring = datestring.strftime("%Y-%m-%d-%H:%M")

            logger.info("LOS final update: %s %s - minimum separation: %f nm %d MSL. Last seen: %s",
                        flight1.flight_id, flight2.flight_id,
                        los.min_latdist, los.min_altdist,
                        datestring)
            Stats.los_finalize += 1

            # do database update
            update_los(flight1, flight2, los.min_latdist, los.min_altdist,
                       los.create_time, los.id)

            # Generate animation if animator is available
            animation_path = None
            if LOS.animator:
                animation_path = _generate_animation(los)

            try:
                # Move to finalized events for post-processing (e.g., animation)
                LOS.finalized_los_events[los.get_key()] = los
                del LOS.current_los_events[los.get_key()]
            except KeyError:
                logger.error("Didn't find key in current_los_events")

            # print CSV record (includes animation path if generated)
            log_csv_record(flight1, flight2, los, datestring, altdatestring,
                          animation_path)
