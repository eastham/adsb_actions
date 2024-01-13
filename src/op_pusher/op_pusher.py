
import signal
import threading
import argparse
import yaml

import sys
sys.path.insert(0, '../adsb_actions')
from bboxes import Bboxes
from flight import Flight
from adsbactions import AdsbActions
import logging

landing_ctr = 0
local_landing_ctr =0
takeoff_ctr = 0

def landing_cb(flight):
    global landing_ctr, local_landing_ctr
    landing_ctr += 1
    if 'note' in flight.flags:
        logging.info("MATCH Local-flight landing detected!")
        local_landing_ctr += 1

def takeoff_cb(flight):
    global takeoff_ctr
    takeoff_ctr += 1

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

    adsb_actions.loop(data=json_data, delay=delay)

if __name__ == '__main__':
    run(None, None)
