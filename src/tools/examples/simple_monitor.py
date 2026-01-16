#!/usr/bin/python3
"""Sample implementation for the library.  

Takes ip address/port/yaml from command line and processes the data.
Use with basic_rules.yaml to print out all traffic."""

from adsb_actions.adsbactions import AdsbActions

import adsb_logger
from adsb_logger import Logger
logger = adsb_logger.logging.getLogger(__name__)
#logger.level = adsb_logger.logging.DEBUG
LOGGER = Logger()

if __name__ == "__main__":
    import argparse

    logger.info('System started.')

    parser = argparse.ArgumentParser(description="match flights against kml bounding boxes")
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument('--test', help="add some test flights", action="store_true")
    parser.add_argument('--ipaddr', help="IP address to connect to", required=True)
    parser.add_argument('--port', help="port to connect to", required=True)
    parser.add_argument('-m', '--mport', type=int, help="metrics port to listen on", default='9107')
    parser.add_argument('yaml', help='Path to the YAML file')
    args = parser.parse_args()

    adsb_actions = AdsbActions(yaml_file=args.yaml, ip=args.ipaddr, port=args.port, mport=args.mport)
    adsb_actions.loop()
 