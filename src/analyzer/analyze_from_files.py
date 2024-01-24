#!/usr/bin/python3
"""Run the rules in analyze_from_files.yaml against a nested
directory structure with readsb data dumps in it."""

import logging
import sys

import yaml

sys.path.insert(0, '../adsb_actions')
from adsbactions import AdsbActions
import replay

YAML_FILE = "./analyze_from_files.yaml"

logger = None

def setup_logger():
    global logger

    logging.basicConfig(level=logging.INFO)
    logging.info('System started.')
    logger = logging.getLogger(__name__)
    logger.level = logging.DEBUG

if __name__ == "__main__":
    import argparse
    setup_logger()

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
 