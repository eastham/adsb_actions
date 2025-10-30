#!/usr/bin/python3
"""Run the rules specified on the command line (default analyze_from_files.yaml)
against a nested directory structure with readsb data dumps in it.  Optionally
perform resampling then proximity checks if --resample is specified."""

import logging
import replay
import datetime
from adsb_actions.adsbactions import AdsbActions
from adsb_actions.adsb_logger import Logger
from op_pusher.abe import process_abe_launch, abe_gc

logger = logging.getLogger(__name__)
# logger.level = logging.DEBUG
LOGGER = Logger()
RESAMPLING_STARTED = False
YAML_FILE = "./analyze_from_files.yaml"

def abe_cb(flight1, flight2):
    """ABE = ADS-B Event -- two airplanes in close proximity"""
    utcstring = datetime.datetime.fromtimestamp(flight1.lastloc.now,
                                                datetime.UTC)
    logger.info("ABE callback: %s %s at %s %d, f1 %f %f f2 %f %f",
                flight1.flight_id, flight2.flight_id,
                utcstring, flight1.lastloc.now,
                flight1.lastloc.lat, flight1.lastloc.lon,
                flight2.lastloc.lat, flight2.lastloc.lon)

    # Ignore ABEs until all data is loaded and we're evaluating the
    # resampled data.
    if RESAMPLING_STARTED:
        process_abe_launch(flight1, flight2, do_threading=False)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=
        "Detect landings/takeoffs/etc from directory of readsb output files.")
    parser.add_argument("-d", "--debug", action="store_true") # XXX not implemented
    parser.add_argument('--yaml', help='Path to the YAML file', default=YAML_FILE)
    parser.add_argument('--resample', action="store_true", help='Enable resampling and proximity checks')
    parser.add_argument('directory', help='Path to the data')
    args = parser.parse_args()

    print("Reading data...")
    allpoints = replay.read_data(args.directory)
    allpoints_iterator = replay.yield_json_data(allpoints)

    print("Processing...")
    adsb_actions = AdsbActions(yaml_file=args.yaml, pedantic=False, resample=args.resample)

    # ad-hoc analysis callbacks from yaml config defined here:
    adsb_actions.register_callback("abe_update_cb", abe_cb)

    adsb_actions.loop(iterator_data = allpoints_iterator)

    if args.resample:
        RESAMPLING_STARTED = True
        prox_events = adsb_actions.do_resampled_prox_checks(abe_gc)
