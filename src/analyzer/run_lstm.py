#!/usr/bin/python3
"""Run the rules in analyze_from_files.yaml against a nested
directory structure with readsb data dumps in it."""

import logging
import replay
import datetime
from lstm import LSTMPipeline
from adsb_actions.adsbactions import AdsbActions
from adsb_actions.adsb_logger import Logger

logger = logging.getLogger(__name__)
logger.level = logging.INFO
LOGGER = Logger()
RESAMPLING_STARTED = False
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

    print("Reading data...")
    allpoints = replay.read_data(args.directory)
    allpoints_iterator = replay.yield_json_data(allpoints)

    print("Processing...")
    adsb_actions = AdsbActions(yaml_file=fn, pedantic=False, resample=True)

    # ad-hoc analysis callbacks here:
    # adsb_actions.register_callback("abe_update_cb", abe_cb)

    adsb_actions.loop(iterator_data = allpoints_iterator)

    RESAMPLING_STARTED = True

    pipeline = LSTMPipeline()

    adsb_actions.resampler.for_each_resampled_point(pipeline.add_aircraft_position)
    adsb_actions.resampler.report_resampling_stats()
    pipeline.run()
