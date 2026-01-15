import logging
from los import process_los_launch, LOS
from db_ops import DATABASE, add_op
from adsb_actions.stats import Stats
from prometheus_client import Gauge

from adsb_actions.adsb_logger import Logger

logger = logging.getLogger(__name__)
#logger.level = logging.DEBUG
LOGGER = Logger()

Stats.register_prom_callbacks()

def landing_cb(flight):
    logger.info("Landing detected! %s", flight.flight_id)
    if 'note' in flight.flags:
        logger.info("Local-flight landing detected! %s", flight.flight_id)
        Stats.local_landings += 1
    Stats.landings += 1

    add_op(flight, "Landing", 'note' in flight.flags)

def popup_takeoff_cb(flight):
    logger.info("Popup takeoff detected! %s", flight.flight_id)
    Stats.popup_takeoffs += 1
    takeoff_cb(flight)

def takeoff_cb(flight):
    logger.info("Takeoff detected! %s", flight.flight_id)
    Stats.takeoffs += 1

    add_op(flight, "Takeoff", False)

def los_cb(flight1, flight2):
    """LOS = Loss of Separation -- two airplanes in close proximity"""
    logger.info("LOS detected! %s", flight1.flight_id)
    process_los_launch(flight1, flight2)

def register_callbacks(adsb_actions):
    adsb_actions.register_callback("landing", landing_cb)
    adsb_actions.register_callback("takeoff", takeoff_cb)
    adsb_actions.register_callback("popup_takeoff", popup_takeoff_cb)
    adsb_actions.register_callback("los_update_cb", los_cb)

def enter_db_fake_mode():
    DATABASE.enter_fake_mode()

def exit_workers():
    LOS.quit = True
    logger.info("Please wait for final LOS GC...")
