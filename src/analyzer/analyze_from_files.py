#!/usr/bin/python3
"""Run the rules in analyze_from_files.yaml against a nested
directory structure with readsb data dumps in it."""

import logging
import replay
from adsb_actions.adsbactions import AdsbActions
from adsb_actions.adsb_logger import Logger
from op_pusher.abe import process_abe_launch, abe_gc

logger = logging.getLogger(__name__)
# logger.level = logging.DEBUG
LOGGER = Logger()

YAML_FILE = "./analyze_from_files.yaml"

def abe_cb(flight1, flight2):
    """ABE = Ads-B Event -- two airplanes in close proximity"""
    logger.info("ABE detected! %s", flight1.flight_id)
    process_abe_launch(flight1, flight2, threading=False)
    ts = flight1.lastloc.now
    abe_gc(ts)

def abes_done(adsb_actions):
    ts = adsb_actions.flights.last_checkpoint
    abe_gc(ts)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=
        "Detect landings/takeoffs/etc from directory of readsb output files.")
    parser.add_argument("-d", "--debug", action="store_true") # XXX not implemented
    parser.add_argument('--yaml', help='Path to the YAML file')
    parser.add_argument('directory', help='Path to the data')
    args = parser.parse_args()

    if args.yaml:
        fn = args.yaml
    else:
        fn = YAML_FILE

    print("Reading data...")
    allpoints = replay.read_data(args.directory)
    allpoints_iterator = replay.yield_json_data(allpoints)

    print("Processing...")
    # XXX pedantic false catches lots less abe's
    adsb_actions = AdsbActions(yaml_file=fn, expire_secs=20, pedantic=True)

    # ad-hoc analysis callbacks here:
    adsb_actions.register_callback("abe_update_cb", abe_cb)

    adsb_actions.loop(iterator_data = allpoints_iterator)

    # ad-hoc analysis completion here:
    abes_done(adsb_actions)
