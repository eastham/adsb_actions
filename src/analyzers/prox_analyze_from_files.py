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
    parser.add_argument('--animation-dir', help='Output directory for animation HTML files (default: examples/generated)')
    parser.add_argument('--export-traffic-samples', help='Export sampled traffic point cloud to CSV file for visualization')
    parser.add_argument('--use-optimizations', action="store_true", default=True,
                        help='Enable performance optimizations for batch processing (default: True)')
    parser.add_argument('--no-optimizations', dest='use_optimizations', action="store_false",
                        help='Disable performance optimizations')
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
    adsb_actions = AdsbActions(yaml_file=args.yaml, pedantic=args.resample,
                              resample=args.resample, use_optimizations=args.use_optimizations)

    # ad-hoc analysis callbacks from yaml config defined here:
    adsb_actions.register_callback("los_update_cb", los_cb)

    adsb_actions.loop(iterator_data = allpoints_iterator)
    adsb_actions.rules.close_emit_files()

    if args.resample:
        RESAMPLING_STARTED = True

        # Set up animator before proximity checks so animations are generated
        # during los_gc() finalization and included in CSV output
        if args.animate_los:
            from postprocessing.los_animator import LOSAnimator
            if args.animation_dir:
                LOS.animation_output_dir = args.animation_dir
            LOS.animator = LOSAnimator(adsb_actions.resampler)
            print(f"Animation generation enabled, output to: {LOS.animation_output_dir}")

        prox_events = adsb_actions.do_resampled_prox_checks(los_gc)

        # Export traffic samples for visualization if requested
        if args.export_traffic_samples:
            print("Exporting traffic point cloud samples...")
            sample_file = args.export_traffic_samples

            # Sample per-flight to ensure all aircraft are represented
            max_samples = 15000  # Per-day limit for reasonable file size and rendering (~4-6MB HTML)

            # Collect positions per flight
            flight_positions = {}
            for timestamp in adsb_actions.resampler.locations_by_time.values():
                for loc in timestamp:
                    if loc.flight not in flight_positions:
                        flight_positions[loc.flight] = []
                    flight_positions[loc.flight].append((loc.lat, loc.lon, loc.alt_baro))

            total_positions = sum(len(positions) for positions in flight_positions.values())
            num_flights = len(flight_positions)

            # Calculate points per flight to hit target
            points_per_flight = max(1, max_samples // num_flights) if num_flights > 0 else 1

            print(f"  Total positions: {total_positions:,} from {num_flights} flights")
            print(f"  Sampling {points_per_flight} points per flight")

            # Collect stats on flight durations and point counts
            flight_stats = []
            for flight_id, positions in flight_positions.items():
                flight_stats.append((flight_id, len(positions)))

            # Sort by position count to see distribution
            flight_stats.sort(key=lambda x: x[1], reverse=True)

            # Stats on distribution
            position_counts = [count for _, count in flight_stats]
            avg_positions = sum(position_counts) / len(position_counts) if position_counts else 0
            median_idx = len(position_counts) // 2
            median_positions = position_counts[median_idx] if position_counts else 0
            print(f"    Average positions per flight: {avg_positions:.0f}")
            print(f"    Median positions per flight: {median_positions}")
            short_flights = sum(1 for count in position_counts if count < 60)  # <1 minute of data
            print(f"    Flights with <60 positions (likely expired or transient): {short_flights}/{num_flights}")

            # Expire statistics from resampler
            if hasattr(adsb_actions.resampler, 'expire_ctr'):
                print(f"    Flight expiration events during resampling: {adsb_actions.resampler.expire_ctr}")
                if num_flights > 0:
                    expire_rate = adsb_actions.resampler.expire_ctr / num_flights
                    print(f"    Average expiration events per flight: {expire_rate:.1f}")

            sample_count = 0
            with open(sample_file, 'w') as f:
                for flight_id, positions in flight_positions.items():
                    # Sample this flight's positions uniformly
                    if len(positions) <= points_per_flight:
                        # Keep all points if flight has few positions
                        sampled = positions
                    else:
                        # Take every Nth point
                        step = len(positions) // points_per_flight
                        sampled = positions[::step][:points_per_flight]

                    for lat, lon, alt in sampled:
                        f.write(f"{lat},{lon},{alt}\n")
                        sample_count += 1

            print(f"Exported {sample_count:,} traffic samples to {sample_file}")
