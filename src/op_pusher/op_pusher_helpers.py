import logging
from abe import process_abe_launch, ABE
from db_ops import DATABASE, add_op
from adsb_actions.stats import Stats
from prometheus_client import Gauge

logger = logging.getLogger(__name__)

op_pusher_helpers_gauge = Gauge('op_pusher_helpers', 'op_pusher_helpers', [ 'message' ])

def landing_cb(flight):
    logging.info("Landing detected! %s", flight.flight_id)
    if 'note' in flight.flags:
        logging.info("Local-flight landing detected! %s", flight.flight_id)
        Stats.local_landings += 1
        op_pusher_helpers_gauge.labels('local landings').set(Stats.local_landings)
    Stats.landings += 1
    op_pusher_helpers_gauge.labels('landings').set(Stats.landings)

    add_op(flight, "Landing", 'note' in flight.flags)

def popup_takeoff_cb(flight):
    Stats.popup_takeoffs += 1
    op_pusher_helpers_gauge.labels('popup takeoffs').set(Stats.popup_takeoffs)
    takeoff_cb(flight)

def takeoff_cb(flight):
    logging.info("Takeoff detected! %s", flight.flight_id)
    Stats.takeoffs += 1
    op_pusher_helpers_gauge.labels('takeoffs').set(Stats.takeoffs)

    add_op(flight, "Takeoff", False)

def abe_cb(flight1, flight2):
    """ABE = Ads-B Event -- two airplanes in close proximity"""
    logging.info("ABE detected! %s", flight1.flight_id)
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
    logging.info("Please wait for final ABE GC...")
