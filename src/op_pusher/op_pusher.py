"""Detect takeoffs/landings and push them to the database."""

import argparse
import sys
import logging
import logging.handlers
from adsb_actions.adsbactions import AdsbActions
import op_pusher_helpers
from prometheus_client import Gauge

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s adsb_actions %(module)s:%(lineno)d: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.handlers.SysLogHandler(),
#        logging.FileHandler("log/op_pusher.log"),
    ]
)

def run():
    logger.info('System started.')

    parser = argparse.ArgumentParser(description="match flights against kml bounding boxes")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument('--test', help="add some test flights", action="store_true")
    parser.add_argument('--ipaddr', help="IP address to connect to")
    parser.add_argument('--port', help="port to connect to")
    parser.add_argument('-m', '--mport', type=int, help="metrics port to listen on", default='9118')
    parser.add_argument('--rules', help="YAML file that describes UI behavior", required=True)
    parser.add_argument('--testdata', help="JSON flight tracks, for testing")
    parser.add_argument('--delay', help="Seconds of delay between reads, for testing", default=0)

    args = parser.parse_args()

    if not bool(args.testdata) != bool(args.ipaddr and args.port):
        logger.fatal("Either ipaddr/port OR testdata must be provided.")
        sys.exit(1)
    if args.ipaddr and args.delay:
        logger.warning("--delay has no effect when ipaddr is given")

    json_data = None
    if not args.testdata:
        adsb_actions = AdsbActions(yaml_file=args.rules, ip=args.ipaddr,
                                   port=args.port, mport=args.mport)
    else:
        with open(args.testdata, 'rt', encoding="utf-8") as myfile:
            json_data = myfile.read()
        adsb_actions = AdsbActions(yaml_file=args.rules)

    op_pusher_helpers.register_callbacks(adsb_actions)

    adsb_actions.loop(delay=float(args.delay), string_data=json_data)
    op_pusher_helpers.exit_workers()

if __name__ == '__main__':
    run()
