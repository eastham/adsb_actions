#!/usr/bin/python3
"""Run the rules in analyze_from_files.yaml against a nested
directory structure with readsb data dumps in it."""

import replay
from adsb_actions.adsbactions import AdsbActions

import adsb_logger
from adsb_logger import Logger

logger = adsb_logger.logging.getLogger(__name__)
#logger.level = adsb_logger.logging.DEBUG
LOGGER = Logger()

YAML_FILE = "./analyze_from_files.yaml"

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

    allpoints = replay.read_data(args.directory)
    allpoints_iterator = replay.yield_json_data(allpoints)

    adsb_actions = AdsbActions(yaml_file=fn)
    adsb_actions.loop(iterator_data = allpoints_iterator)
 