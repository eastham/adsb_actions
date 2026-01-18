"""
Basic analyzer to run rules on flight data, either live or from a saved
set of readsb data files.  This also auto-loads callback definitions from
a specified Python file.

Usage: generic_analyzer.py [--ipaddr=IPADDR --port=PORT]
    [--data=DATA_DIRECTORY] [--callback_definitions=FILE.PY] [--delay=SECONDS]
    RULES_YAML_FILE

Example: python3 generic_analyzer.py --data ../../tests/sample_readsb_data 
--callback_definitions=example_callbacks.py example_rules.yaml 
"""

import argparse
import importlib.util
import logging

import yaml
from lib import replay
import sys
from adsb_actions.adsbactions import AdsbActions
from adsb_actions.adsb_logger import Logger

logger = logging.getLogger(__name__)
logger.level = logging.INFO

def parseargs():
    parser = argparse.ArgumentParser(
        description="render a simple flight status board.")
    parser.add_argument('rules', help="rules.yaml file to use")
    parser.add_argument('--ipaddr', help="IP address to connect to",
                        default="127.0.0.1")
    parser.add_argument('--port', help="port to connect to")
    parser.add_argument('--callback_definitions',
                        help="callback definitions file")
    parser.add_argument('--data', help="readsb data files for analysis")
    parser.add_argument(
        '--delay', help="Seconds of delay between reads, for testing", default=0)
    args = parser.parse_args()

    if not bool(args.data) != bool(args.ipaddr and args.port):
        logger.fatal("Either ipaddr/port OR testdata must be provided.")
        sys.exit(1)
    if args.ipaddr and args.delay:
        logger.warning("--delay has no effect when ipaddr is given")

    return args


def setup():
    logger.info('System started.')

    args = parseargs()

    with open(args.rules, 'r', encoding='utf-8') as file:
        yaml_data = yaml.safe_load(file)

    # Setup flight data handling.

    allpoints_iterator = None
    if not args.data:
        adsb_actions = AdsbActions(yaml_data, ip=args.ipaddr, port=args.port)
    else:
        adsb_actions = AdsbActions(yaml_data)

        print("Reading data...")
        allpoints = replay.read_data(args.data)
        allpoints_iterator = replay.yield_json_data(allpoints)

        print("Processing...")
        adsb_actions = AdsbActions(yaml_file=args.rules, pedantic=False)

    # Import callback definitions file
    if args.callback_definitions:
        spec = importlib.util.spec_from_file_location(
            "callbacks", args.callback_definitions)
        callbacks = importlib.util.module_from_spec(spec)
        sys.modules["callbacks"] = callbacks
        spec.loader.exec_module(callbacks)

        # Expose all functions from the callbacks module to adsb_actions
        for name in dir(callbacks):
            if not name.startswith('_') and name !='Logger':
                adsb_actions.register_callback(name, getattr(callbacks, name))

    # Run the processing loop
    adsb_actions.loop(iterator_data=allpoints_iterator, delay=float(args.delay))


if __name__ == '__main__':
    setup()
