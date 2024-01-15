#!/usr/bin/python3
"""Sample implementation for the library.  

Takes ip address/port/yaml from command line and processes the data."""

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
        "detect landings/takeoffs/etc from directory of readsb output files.")
    parser.add_argument("-d", "--debug", action="store_true") # XXX not implemented
    parser.add_argument('directory', help='Path to the data')
    args = parser.parse_args()

    with open(YAML_FILE, 'r', encoding='utf-8') as file:
        yaml_data = yaml.safe_load(file)

    allpoints = replay.read_data(args.directory)
    allpoints_iterator = replay.yield_json_data(allpoints)

    adsb_actions = AdsbActions(yaml_data)
    adsb_actions.loop(iterator_data = allpoints_iterator)
 