"""Iterate through all ADSBexchange-disk-format data found under the given path
and report on how many positions were seen per day.

Usage: python3 data_verifier.py [--by_hour] [--print_every PRINT_EVERY] directory

options:
  --by_hour             Bucket data by hour, not just date
  --print_every PRINT_EVERY
                        Print out every nth entry
"""

import replay
import argparse
import datetime

def get_adsbx_iterable(directory: str):
    allpoints = replay.read_data(directory)
    allpoints_iterable = replay.yield_json_data(allpoints,
                                                insert_dummy_entries=False)
    return allpoints_iterable

def count_points(allpoints_iterable, by_hour: bool, print_every: int = -1) -> int:
    point_ctr_by_date = {}
    total_points = 0

    for point in allpoints_iterable:
        total_points += 1
        date = timestamp_to_datestring(point['now'], by_hour)
        if date in point_ctr_by_date:
            point_ctr_by_date[date] += 1
        else:
            point_ctr_by_date[date] = 1

        if print_every > 0 and total_points % print_every == 0:
            print(point)

    for k,v in point_ctr_by_date.items():
        print(f"{k}: {v} points")
    print(f"Total points: {total_points}")
    return total_points

def timestamp_to_datestring(ts: int, include_hour: bool) -> str:
    utctime = datetime.datetime.utcfromtimestamp(ts)
    formatted_time = utctime.strftime('%m/%d')
    if include_hour:
        formatted_time += utctime.strftime(' %H:00')
    return formatted_time

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Argument Parser for Constants')

    parser.add_argument('directory', type=str, help='Directory to scan')
    parser.add_argument("--by_hour", action="store_true",
                        help="Bucket data by hour, not just date")
    parser.add_argument('--print_every', type=int, default=-1,
                        help='Print out every nth entry')

    args = parser.parse_args()

    print("Loading all data files...")
    adsbx_iterable = get_adsbx_iterable(args.directory)

    print("Parsing data...")
    count_points(adsbx_iterable, args.by_hour, args.print_every)
