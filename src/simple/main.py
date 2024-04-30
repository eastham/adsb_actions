#!/usr/bin/python3
"""Sample implementation for the library.  

Takes ip address/port/yaml from command line and processes the data.
Use with basic_rules.yaml to print out all traffic."""

import logging
from adsb_actions.adsbactions import AdsbActions

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

    setup_logger()

    adsb_actions = AdsbActions(yaml_file=args.yaml, ip=args.ipaddr, port=args.port, mport=args.mport)
    adsb_actions.loop()
 
