import logging
from abe import process_abe_launch, ABE
from db_ops import DATABASE, add_op
from adsb_actions.stats import Stats
from prometheus_client import Gauge

from adsb_actions.adsb_logger import Logger

logger = logging.getLogger(__name__)
#logger.level = logging.DEBUG
LOGGER = Logger()
EXCLUDE_SUBSTRS = ["N10C", "N10D"]  # XXX HACK

Stats.register_prom_callbacks()

def check_exclusions(flight):
    for substr in EXCLUDE_SUBSTRS:
        if substr in flight.flight_id:
            logger.debug("Excluded flight %s", flight.flight_id)
            return True
    return False

def landing_cb(flight):
    if check_exclusions(flight):
        return

    logger.info("Landing detected! %s", flight.flight_id)
    if 'note' in flight.flags:
        logger.info("Local-flight landing detected! %s", flight.flight_id)
        Stats.local_landings += 1
    Stats.landings += 1

    add_op(flight, "Landing", 'note' in flight.flags)

def popup_takeoff_cb(flight):
    if check_exclusions(flight):
        return

    logger.info("Popup takeoff detected! %s", flight.flight_id)
    Stats.popup_takeoffs += 1
    takeoff_cb(flight)

def takeoff_cb(flight):
    if check_exclusions(flight):
        return

    logger.info("Takeoff detected! %s", flight.flight_id)
    Stats.takeoffs += 1

    add_op(flight, "Takeoff", False)

def abe_cb(flight1, flight2):
    """ABE = Ads-B Event -- two airplanes in close proximity"""
    if check_exclusions(flight1) or check_exclusions(flight2):
        return
    logger.info("ABE detected! %s", flight1.flight_id)
    process_abe_launch(flight1, flight2)

def register_callbacks(adsb_actions):
    adsb_actions.register_callback("landing", landing_cb)
    adsb_actions.register_callback("takeoff", takeoff_cb)
    adsb_actions.register_callback("popup_takeoff", popup_takeoff_cb)
    adsb_actions.register_callback("abe_update_cb", abe_cb)

def enter_db_fake_mode():
    DATABASE.enter_fake_mode()

def exit_workers():
    ABE.quit = True
    logger.info("Please wait for final ABE GC...")
