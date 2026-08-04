#!/usr/bin/env python3
"""Sample implementation for the library.

Takes ip address/port/yaml from command line and processes the data,
or reads from a directory of readsb data files.

Usage:
    python3 src/analyzers/simple_monitor.py --directory tests/sample_readsb_data examples/hello_world_rules.yaml

For examples that use callbacks, see callback_runner.py instead.
"""

from adsb_actions.adsbactions import AdsbActions
from adsb_actions import adsb_logger
from adsb_actions.adsb_logger import Logger
from lib import replay

logger = adsb_logger.logging.getLogger(__name__)
#logger.level = adsb_logger.logging.DEBUG
LOGGER = Logger()


def print_aircraft_data(flight):
    """Example callback that prints full flight data."""
    print(f"print_aircraft_data callback triggered: {flight.to_str()}")

if __name__ == "__main__":
    import argparse

    logger.info('System started.')

    parser = argparse.ArgumentParser(description="match flights against kml bounding boxes")
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument('--test', help="add some test flights", action="store_true")
    parser.add_argument('--ipaddr', help="IP address to connect to")
    parser.add_argument('--port', help="port to connect to")
    parser.add_argument('--directory', help="directory of readsb data files to read from")
    parser.add_argument(
        '--sorted-file', help='Path to time-sorted JSONL file (.json, .jsonl, or .gz)')
    parser.add_argument('-m', '--mport', type=int, help="metrics port to listen on", default=None)
    parser.add_argument('yaml', help='Path to the YAML file')
    args = parser.parse_args()

    if args.sorted_file:
        print(f"Streaming from sorted file: {args.sorted_file}")
        allpoints_iterator = replay.yield_from_sorted_file(args.sorted_file)
        adsb_actions = AdsbActions(yaml_file=args.yaml, mport=args.mport)
        adsb_actions.register_callback(
            "print_aircraft_data", print_aircraft_data)
        adsb_actions.loop(iterator_data=allpoints_iterator)
    elif args.directory:
        # File-based replay mode
        print("Reading data from directory...")
        allpoints = replay.read_data(args.directory)
        allpoints_iterator = replay.yield_json_data(allpoints)
        print("Processing...")
        adsb_actions = AdsbActions(yaml_file=args.yaml, mport=args.mport)
        adsb_actions.register_callback("print_aircraft_data", print_aircraft_data)
        adsb_actions.loop(iterator_data=allpoints_iterator)
    elif args.ipaddr and args.port:
        # Network mode
        adsb_actions = AdsbActions(yaml_file=args.yaml, ip=args.ipaddr, port=args.port, mport=args.mport)
        adsb_actions.register_callback("print_aircraft_data", print_aircraft_data)
        adsb_actions.loop()
    else:
        parser.error("Either --directory, --sorted-file, or both --ipaddr and --port are required")
 