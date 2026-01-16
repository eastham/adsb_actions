#!/usr/bin/python3
"""Run the rules specified on the command line (default analyze_from_files.yaml)
against a nested directory structure with readsb data dumps in it."""

import logging
import replay
import datetime
from adsb_actions.adsbactions import AdsbActions
from adsb_actions.adsb_logger import Logger
from op_pusher.los import process_los_launch, los_gc

logger = logging.getLogger(__name__)
# logger.level = logging.DEBUG
LOGGER = Logger()
RESAMPLING_STARTED = False
YAML_FILE = "./analyze_from_files.yaml"

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=
        "Detect landings/takeoffs/etc from directory of readsb output files.")
    parser.add_argument("-d", "--debug", action="store_true") # XXX not implemented
    parser.add_argument('--yaml', help='Path to the YAML file', default=YAML_FILE)
    parser.add_argument('directory', help='Path to the data')
    args = parser.parse_args()

    print("Reading data...")
    allpoints = replay.read_data(args.directory)
    allpoints_iterator = replay.yield_json_data(allpoints)

    print("Processing...")
    adsb_actions = AdsbActions(yaml_file=args.yaml, pedantic=False)

    # ad-hoc analysis callbacks from yaml config defined here, if desired...
    # i.e. adsb_actions.register_callback("takeoff_cb", takeoff)

    adsb_actions.loop(iterator_data = allpoints_iterator)
