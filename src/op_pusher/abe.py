"""Push ABE's (ADS-B Events) to the server.  
These are pushed once upon first detection and again once
expired, so that the minimum distance is logged."""

import sys
import logging
import threading
import time

sys.path.insert(0, '../db')
import db_ops
from stats import Stats

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG

class ABE:
    """ 
    Track ABE events.  These are pushed to the server when initially seen,
    updated locally when additional callbacks come in, and re-pushed to the
    server with the final stats once the event is gc'ed.
    """
    current_abes = {}
    current_abe_lock: threading.Lock = threading.Lock()
    ABE_GC_TIME = 60
    gc_thread = None
    quit = False

    def __init__(self, flight1, flight2, latdist, altdist, create_time):
        # keep these in a universal order to enforce lock ordering and consistent keys
        if flight1.flight_id > flight2.flight_id:
            self.flight2 = flight1
            self.flight1 = flight2
        else:
            self.flight1 = flight1
            self.flight2 = flight2
        self.latdist = self.min_latdist = latdist
        self.altdist = self.min_altdist = altdist
        self.create_time = self.last_time = create_time
        self.id = None

    def update(self, latdist, altdist, last_time):
        self.latdist = latdist
        self.altdist = altdist
        self.last_time = last_time
        # perhaps this is better done with an absolute distance?
        if latdist <= self.min_latdist or altdist <= self.min_altdist:
            self.min_latdist = latdist
            self.min_altdist = altdist

    def key(self):
        key = "%s %s" % (self.flight1.flight_id.strip(),
            self.flight2.flight_id.strip())
        return key

def process_abe_launch(flight1, flight2):
    t = threading.Thread(target=process_abe, args=[flight1, flight2])
    t.start()

def process_abe(flight1, flight2):
    if not ABE.gc_thread:
        ABE.gc_thread = threading.Thread(target=gc_loop)
        ABE.gc_thread.start()

    logger.debug("ABE_CB " + flight1.flight_id + " " + flight2.flight_id)

    lateral_distance = flight1.lastloc - flight2.lastloc
    alt_distance = abs(flight1.lastloc.alt_baro - flight2.lastloc.alt_baro)
    now = flight1.lastloc.now
    # always create a new ABE at least to get flight1/flight2 ordering right
    abe = ABE(flight1, flight2, lateral_distance, alt_distance, now)

    ABE.current_abe_lock.acquire()
    key = abe.key()
    if key in ABE.current_abes:
        logger.debug("ABE update " + key)
        ABE.current_abes[key].update(lateral_distance, alt_distance, now)
        Stats.abe_update += 1
        ABE.current_abe_lock.release()
    else:
        logger.debug("ABE add " + key)
        ABE.current_abes[key] = abe
        Stats.abe_add += 1
        ABE.current_abe_lock.release()

        abe.id = db_ops.add_abe(flight1, flight2, lateral_distance, alt_distance)

def gc_loop():
    while True:
        time.sleep(10)
        abe_gc()
        if ABE.quit: return

def abe_gc():
    logger.debug("ABE_GC")
    with ABE.current_abe_lock:
        abe_list = list(ABE.current_abes.values())

    for abe in abe_list:
        logger.debug(f"ABE_GC {abe.key()} {time.time()} {abe.last_time}")

        # NOTE: time.time() doesn't behave correctly here in replay mode.
        if time.time() - abe.last_time > ABE.ABE_GC_TIME:
            logger.debug(f"ABE final update {abe.flight1.flight_id} {abe.flight2.flight_id}")
            Stats.abe_finalize += 1

            db_ops.update_abe(abe.flight1, abe.flight2,
                abe.min_latdist, abe.min_altdist, abe.create_time, abe.id)
            try:
                del ABE.current_abes[abe.key()]
            except Exception:
                print("Error: didn't find key in current_abes")
                pass