"""Push ABE's (ADS-B Events) to the server.  
These are pushed once upon first detection and again once
expired, so that the minimum distance is logged."""

import copy
import logging
import threading
import time
import datetime

from op_pusher.db_ops import add_abe, update_abe
from adsb_actions.stats import Stats
from adsb_actions.location import Location
from adsb_actions.adsb_logger import Logger

logger = logging.getLogger(__name__)
#logger.level = logging.DEBUG
LOGGER = Logger()

class ABE:
    """ 
    Track ABE events.  These are pushed to the server when initially seen,
    updated locally when additional callbacks come in, and re-pushed to the
    server with the final stats once the event is gc'ed.
    """
    current_abes = {}
    current_abe_lock: threading.Lock = threading.Lock()
    ABE_GC_TIME = 60        # seconds to wait before finalizing ABE
    ABE_GC_LOOP_DELAY = 1   # seconds between GC checks
    gc_thread = None
    quit = False

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
        self.id = None

    def update(self, latdist, altdist, last_time, flight1, flight2,
               update_loc_at_closest_approach=True):
        self.latdist = latdist
        self.altdist = altdist
        self.last_time = last_time

        if latdist <= self.min_latdist or altdist <= self.min_altdist:
            self.min_latdist = latdist
            self.min_altdist = altdist
            if update_loc_at_closest_approach:
                self.first_loc_1 = copy.deepcopy(flight1.lastloc)
                self.first_loc_2 = copy.deepcopy(flight2.lastloc)

    def get_key(self):
        key = "%s %s" % (self.flight1.flight_id.strip(),
            self.flight2.flight_id.strip())
        return key

def process_abe_launch(flight1, flight2, do_threading=True):
    """Saw an ABE, start a thread to process (so as not to block the caller).
    NOTE: if not using threading, implementor must call abe_gc() periodically,
    best to do it with a rule_cooldown rule."""
    if do_threading:
        t = threading.Thread(target=process_abe, args=[flight1, flight2])
        t.start()

        if not ABE.gc_thread:
            ABE.gc_thread = threading.Thread(target=gc_loop)
            ABE.gc_thread.start()
    else:
        process_abe(flight1, flight2)

def process_abe(flight1, flight2):
    """Handle a single ABE.  Could be new, or just an update to one that's 
    already underway.  Push to external database if new."""

    logger.info("process_abe " + flight1.flight_id + " " + flight2.flight_id)

    lateral_distance = flight1.lastloc - flight2.lastloc
    alt_distance = abs(flight1.lastloc.alt_baro - flight2.lastloc.alt_baro)
    now = flight1.lastloc.now
    # always create a new ABE at least to get flight1/flight2 ordering right
    abe = ABE(flight1, flight2, lateral_distance, alt_distance, now)

    with ABE.current_abe_lock:
        key = abe.get_key()
        if key in ABE.current_abes:
            logger.debug("ABE update of key %s", key)
            ABE.current_abes[key].update(lateral_distance, alt_distance, now,
                                         flight1, flight2)
            Stats.abe_update += 1
        else:
            logger.debug("ABE add key "+ key +" at " +
                         str(datetime.datetime.utcfromtimestamp(now)) +
                         ": " + flight1.to_str() + " " + flight2.to_str())
            ABE.current_abes[key] = abe
            Stats.abe_add += 1

            abe.id = add_abe(flight1, flight2, lateral_distance,
                               alt_distance)

def gc_loop():
    """Run in a separate thread to periodically check for ABE's that are
    ready to be finalized."""
    while True:
        time.sleep(ABE.ABE_GC_LOOP_DELAY)
        abe_gc(time.time())
        if ABE.quit: return

def abe_gc(ts):
    """Check if any ABEs are ready to be finalized (i.e. final stats recorded)"""

    with ABE.current_abe_lock:
        abe_list = list(ABE.current_abes.values())

    for abe in abe_list:
        logger.debug(f"ABE_GC {abe.get_key()} {ts} {abe.last_time}")
        flight1 = abe.flight1
        flight2 = abe.flight2

        if ts - abe.last_time > ABE.ABE_GC_TIME:
            # No updates to this ABE for a while, finalize to database and remove.
            logger.info("ABE final update: %s %s - minimum separation: %f nm %d MSL. Last seen: %s",
                        flight1.flight_id, flight2.flight_id,
                        abe.min_latdist, abe.min_altdist,
                        datetime.datetime.utcfromtimestamp(abe.last_time))
            Stats.abe_finalize += 1

            # do database update
            update_abe(flight1, flight2, abe.min_latdist, abe.min_altdist,
                       abe.create_time, abe.id)
            try:
                del ABE.current_abes[abe.get_key()]
            except KeyError:
                logger.error("Didn't find key in current_abes")

            # print CSV record
            meanloc = Location.meanloc(abe.first_loc_1, abe.first_loc_2)
            logger.info("%d,%f,%f,%d,%s,%s,%d,%d,%f,%d",
                flight1.lastloc.now, meanloc.lat, meanloc.lon,
                meanloc.alt_baro,
                flight1.flight_id.strip(), flight2.flight_id.strip(),
                flight1.lastloc.gs, flight2.lastloc.gs,
                abe.min_latdist, abe.min_altdist)
