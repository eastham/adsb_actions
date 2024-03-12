"""Detect takeoffs/landings and push them to the database."""

import argparse
import sys
import logging
import abe

sys.path.insert(0, '../db')
from adsbactions import AdsbActions
import db_ops
from stats import Stats

logger = logging.getLogger(__name__)

def landing_cb(flight):
    logging.info("Landing detected! %s", flight.flight_id)
    if 'note' in flight.flags:
        logging.info("Local-flight landing detected! %s", flight.flight_id)
        Stats.local_landings += 1
    Stats.landings += 1

    db_ops.add_op(flight, "Landing", 'note' in flight.flags)

def popup_takeoff_cb(flight):
    Stats.popup_takeoffs += 1
    takeoff_cb(flight)

def takeoff_cb(flight):
    logging.info("Takeoff detected! %s", flight.flight_id)
    Stats.takeoffs += 1

    db_ops.add_op(flight, "Takeoff", False)

def abe_cb(flight1, flight2):
    """ABE = Ads-B Event -- two airplanes in close proximity"""
    logging.info("ABE detected! %s", flight1.flight_id)
    abe.process_abe_launch(flight1, flight2)

def register_callbacks(adsb_actions):
    adsb_actions.register_callback("landing", landing_cb)
    adsb_actions.register_callback("takeoff", takeoff_cb)
    adsb_actions.register_callback("popup_takeoff", popup_takeoff_cb)
    adsb_actions.register_callback("abe_update_cb", abe_cb)

def enter_db_fake_mode():
    db_ops.DATABASE.enter_fake_mode()

def exit_workers():
    abe.ABE.quit = True
    logging.info("Please wait for final ABE GC...")

def run():
    logging.basicConfig(format='%(levelname)-8s %(module)s:%(lineno)d: %(message)s',
                        level=logging.INFO)
    logging.info('System started.')

    parser = argparse.ArgumentParser(description="match flights against kml bounding boxes")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument('--test', help="add some test flights", action="store_true")
    parser.add_argument('file', nargs='+', help="kml files to use")
    parser.add_argument('--ipaddr', help="IP address to connect to")
    parser.add_argument('--port', help="port to connect to")
    parser.add_argument('--rules', help="YAML file that describes UI behavior", required=True)
    parser.add_argument('--testdata', help="JSON flight tracks, for testing")
    parser.add_argument('--delay', help="Seconds of delay between reads, for testing", default=0)

    args = parser.parse_args()

    if not bool(args.testdata) != bool(args.ipaddr and args.port):
        logger.fatal("Either ipaddr/port OR testdata must be provided.")
        sys.exit(1)
    if args.ipaddr and args.delay:
        logger.warning("--delay has no effect when ipaddr is given")

    json_data = None
    if not args.testdata:
        adsb_actions = AdsbActions(yaml_file=args.rules, ip=args.ipaddr,
                                   port=args.port)
    else:
        with open(args.testdata, 'rt', encoding="utf-8") as myfile:
            json_data = myfile.read()
        adsb_actions = AdsbActions(yaml_file=args.rules)

    register_callbacks(adsb_actions)

    adsb_actions.loop(delay=float(args.delay), string_data=json_data)
    exit_workers()

if __name__ == '__main__':
    run()
