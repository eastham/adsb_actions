#!/usr/bin/env python3

from batch_los_pipeline import download_tar_parts
from batch_helpers import (generate_date_range, validate_date)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Download ADS-B data from ADSB.LOL for specified dates")
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD inclusive)')
    parser.add_argument('--day-filter', choices=['all', 'weekday', 'weekend'], default='all', help='Filter dates by day type (default: all)')
    parser.add_argument('--data-dir', default='data/adsb_lol', help='Directory to save downloaded data')
    parser.add_argument('--dry-run', action='store_true', help='Print dates to download without actually downloading'   )
    args = parser.parse_args()

    start_date = validate_date(args.start_date)
    end_date = validate_date(args.end_date)
    dates = generate_date_range(start_date, end_date, args.day_filter)
    
    for date in dates:
        date_iso = date.strftime('%Y.%m.%d')
        if args.dry_run:
            print(f"Would download data for {date_iso}")
        else:
            download_tar_parts(date, args.data_dir)

        print(f"Downloaded data for {date_iso} saved to {args.data_dir}/v{date_iso}-planes-readsb-prod-0.tar.aa and .tar.ab")