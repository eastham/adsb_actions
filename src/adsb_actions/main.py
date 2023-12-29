#!/usr/bin/python3

import logging
import sys
from pathlib import Path

import yaml
from adsbactions import AdsbActions

path_root = Path(__file__).parents[0]
sys.path.append(str(path_root))
logger = None

def setup_logger():
    global logger

    logging.basicConfig(level=logging.INFO)
    logging.info('System started.')
    logger = logging.getLogger(__name__)
    logger.level = logging.DEBUG

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="match flights against kml bounding boxes")
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument('--test', help="add some test flights", action="store_true")
    parser.add_argument('--ipaddr', help="IP address to connect to", required=True)
    parser.add_argument('--port', help="port to connect to", required=True)
    parser.add_argument('yaml', help='Path to the YAML file')
    args = parser.parse_args()

    with open(args.yaml, 'r', encoding='utf-8') as file:
        yaml_data = yaml.safe_load(file)
    print(yaml_data)
    setup_logger()

    adsb_actions = AdsbActions(yaml_data, ip=args.ipaddr, port=args.port)
    adsb_actions.loop()
 