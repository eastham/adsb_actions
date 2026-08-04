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

        LOS.resampler = adsb_actions.resampler
        prox_events = adsb_actions.do_resampled_prox_checks(los_gc)

        # Export traffic samples for visualization if requested
        if args.export_traffic_samples:
            print("Exporting traffic tracks...")
            sample_file = args.export_traffic_samples

            # Collect complete tracks per flight (preserve temporal order)
            # Use locations_by_time which includes both original AND interpolated points
            # This gives smooth tracks instead of jagged sparse reports
            flight_tracks = {}
            for timestamp in sorted(adsb_actions.resampler.locations_by_time.keys()):
                for loc in adsb_actions.resampler.locations_by_time[timestamp]:
                    flight_id = loc.flight
                    if flight_id not in flight_tracks:
                        flight_tracks[flight_id] = []
                    flight_tracks[flight_id].append((loc.lat, loc.lon, loc.alt_baro, loc.track))

            # Apply heading-change decimation to reduce points while preserving path shape
            # Only keep points where the aircraft turns significantly
            def decimate_track(track, min_heading_change=5.0):
                """Keep points where heading changes significantly or at start/end."""
                if len(track) <= 2:
                    return track

                decimated = [track[0]]  # Always keep first point
                last_kept_heading = track[0][3]  # track field from first point

                for i in range(1, len(track) - 1):
                    curr_heading = track[i][3]

                    # Calculate heading change since last kept point
                    heading_diff = abs(curr_heading - last_kept_heading)
                    if heading_diff > 180:
                        heading_diff = 360 - heading_diff

                    # Keep point if heading changed significantly since last kept point
                    if heading_diff >= min_heading_change:
                        decimated.append(track[i])
                        last_kept_heading = curr_heading

                decimated.append(track[-1])  # Always keep last point
                return decimated

            # Statistics
            total_points_before = sum(len(track) for track in flight_tracks.values())
            num_flights = len(flight_tracks)

            print(f"  Total track points before decimation: {total_points_before:,} from {num_flights} flights")

            # Decimate tracks and collect stats
            decimated_tracks = {}
            total_points_after = 0
            flight_stats = []

            for flight_id, track in flight_tracks.items():
                decimated = decimate_track(track, min_heading_change=10.0)  # Keep points with 10° turns
                decimated_tracks[flight_id] = decimated
                total_points_after += len(decimated)
                flight_stats.append((flight_id, len(track), len(decimated)))

            reduction_pct = (1 - total_points_after / total_points_before) * 100 if total_points_before > 0 else 0
            print(f"  After heading-change decimation: {total_points_after:,} points ({reduction_pct:.1f}% reduction)")

            # Show flight stats
            flight_stats.sort(key=lambda x: x[1], reverse=True)  # Sort by original point count
            avg_before = total_points_before / num_flights if num_flights > 0 else 0
            avg_after = total_points_after / num_flights if num_flights > 0 else 0
            print(f"    Average points per flight: {avg_before:.0f} → {avg_after:.0f}")

            short_flights = sum(1 for _, orig, _ in flight_stats if orig < 60)
            print(f"    Flights with <60 positions (likely expired or transient): {short_flights}/{num_flights}")

            # Expire statistics from resampler
            if hasattr(adsb_actions.resampler, 'expire_ctr'):
                print(f"    Flight expiration events during resampling: {adsb_actions.resampler.expire_ctr}")
                if num_flights > 0:
                    expire_rate = adsb_actions.resampler.expire_ctr / num_flights
                    print(f"    Average expiration events per flight: {expire_rate:.1f}")

            # Write tracks to file (one track per line, JSON format for easy parsing)
            import json
            with open(sample_file, 'w') as f:
                for flight_id, track in decimated_tracks.items():
                    # Format: [[lat, lon, alt], [lat, lon, alt], ...]
                    coords = [[lat, lon, alt] for lat, lon, alt, _ in track]
                    f.write(json.dumps(coords) + '\n')

            print(f"Exported {num_flights} flight tracks with {total_points_after:,} total points to {sample_file}")
