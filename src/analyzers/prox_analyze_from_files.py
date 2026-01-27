#!/usr/bin/python3
"""Run the rules specified on the command line (default analyze_from_files.yaml)
against a nested directory structure with readsb data dumps in it.  Optionally
perform resampling then proximity checks if --resample is specified.

For large datasets, use --sorted-file with a preprocessed file from convert_traces.py
to stream data with minimal memory usage."""

import datetime
import logging
from lib import replay
from adsb_actions.adsbactions import AdsbActions
from adsb_actions.adsb_logger import Logger
from applications.airport_monitor.los import process_los_launch, los_gc, LOS

logger = logging.getLogger(__name__)
# logger.level = logging.DEBUG
LOGGER = Logger()
RESAMPLING_STARTED = False
YAML_FILE = "./analyze_from_files.yaml" # XXX???

def los_cb(flight1, flight2):
    """LOS = Loss of Separation -- two airplanes in close proximity"""
    utcstring = datetime.datetime.fromtimestamp(flight1.lastloc.now,
                                                datetime.UTC)
    logger.info("LOS callback: %s %s at %s %d, f1 %f %f f2 %f %f",
                flight1.flight_id, flight2.flight_id,
                utcstring, flight1.lastloc.now,
                flight1.lastloc.lat, flight1.lastloc.lon,
                flight2.lastloc.lat, flight2.lastloc.lon)

    # Ignore LOS events until all data is loaded and we're evaluating the
    # resampled data.
    if RESAMPLING_STARTED:
        process_los_launch(flight1, flight2, do_threading=False)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=
        "Detect landings/takeoffs/etc from directory of readsb output files.")
    parser.add_argument("-d", "--debug", action="store_true") # XXX not implemented
    parser.add_argument('--yaml', help='Path to the YAML file', default=YAML_FILE)
    parser.add_argument('--resample', action="store_true", help='Enable resampling and proximity checks')
    parser.add_argument('--sorted-file', help='Path to time-sorted JSONL file (.json, .jsonl, or .gz)')
    parser.add_argument('--animate-los', action="store_true", help='Generate animated HTML maps for each LOS event')
    parser.add_argument('directory', nargs='?', help='Path to the data (not needed if --sorted-file used)')
    args = parser.parse_args()

    if args.sorted_file:
        # Stream from preprocessed sorted file
        print(f"Streaming from sorted file: {args.sorted_file}")
        allpoints_iterator = replay.yield_from_sorted_file(args.sorted_file)
    elif args.directory:
        # Original behavior: load all into memory
        print("Reading data...")
        allpoints = replay.read_data(args.directory)
        allpoints_iterator = replay.yield_json_data(allpoints)
    else:
        parser.error("Either directory or --sorted-file is required")

    print("Processing...")
    adsb_actions = AdsbActions(yaml_file=args.yaml, pedantic=True, resample=args.resample)

    # ad-hoc analysis callbacks from yaml config defined here:
    adsb_actions.register_callback("los_update_cb", los_cb)

    adsb_actions.loop(iterator_data = allpoints_iterator)

    if args.resample:
        RESAMPLING_STARTED = True

        # Set up animator before proximity checks so animations are generated
        # during los_gc() finalization and included in CSV output
        if args.animate_los:
            from postprocessing.los_animator import LOSAnimator
            LOS.animator = LOSAnimator(adsb_actions.resampler)
            print(f"Animation generation enabled, output to: {LOS.animation_output_dir}")

        prox_events = adsb_actions.do_resampled_prox_checks(los_gc)
