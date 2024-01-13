"""Detect takeoffs/landings and push them to the database."""

import argparse
import sys
import logging
import yaml
import abe
import signal

sys.path.insert(0, '../db')
from adsbactions import AdsbActions
import db_ops
from stats import Stats

def landing_cb(flight):
    logging.info("Landing detected! %s", flight.flight_id)
    if 'note' in flight.flags:
        logging.info("Local-flight landing detected! %s", flight.flight_id)
        Stats.local_landings += 1
    Stats.landings += 1

    db_ops.add_op(flight, "Landing", 'note' in flight.flags)

def takeoff_cb(flight):
    logging.info("Takeoff detected! %s", flight.flight_id)
    Stats.takeoffs += 1

    db_ops.add_op(flight, "Takeoff", False)

def abe_cb(flight1, flight2):
    """ABE = Ads-B Event -- for example two airplanes getting in close proximity"""
    logging.info("ABE detected! %s", flight1.flight_id)
    abe.process_abe_launch(flight1, flight2)

def run(focus_q, admin_q):
    parser = argparse.ArgumentParser(description="match flights against kml bounding boxes")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument('--test', help="add some test flights", action="store_true")
    parser.add_argument('file', nargs='+', help="kml files to use")
    parser.add_argument('--ipaddr', help="IP address to connect to", required=True)
    parser.add_argument('--port', help="port to connect to", required=True)
    parser.add_argument('--rules', help="YAML file that describes UI behavior", required=True)
    parser.add_argument('--testdata', help="JSON flight tracks, for testing")

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    args = parser.parse_args()
    with open(args.rules, 'r', encoding='utf-8') as file:
        yaml_data = yaml.safe_load(file)

    json_data = None
    delay = 0 # used for testing to slow down replay rate
    if not args.testdata:
        adsb_actions = AdsbActions(yaml_data, ip=args.ipaddr, port=args.port)
    else:
        adsb_actions = AdsbActions(yaml_data)

        with open(args.testdata, 'rt', encoding="utf-8") as myfile:
            json_data = myfile.read()
        delay = .0

    adsb_actions.register_callback("landing", landing_cb)
    adsb_actions.register_callback("takeoff", takeoff_cb)
    adsb_actions.register_callback("abe_update_cb", abe_cb)

    adsb_actions.loop(data=json_data, delay=delay)
    abe.ABE.quit = True # signal worker thread to exit
    logging.info("Please wait for final ABE GC...")

if __name__ == '__main__':
    run(None, None)
