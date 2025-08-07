"""Calculate the week before Labor Day for a given year and run analyze_from_files
script for each day in that week.
This script is designed to be run from the command line with the year as an argument.
Example usage:
python runall.py -y 2023
"""

from datetime import date, timedelta

ANALYZE_CMD = "/Users/eastham/git2/adsb_actions/.venv/bin/python3 ./analyze_from_files.py --yaml prox_analyze_from_files.yaml"
INPUT_DIR = "/Users/eastham/adsb_data/"
OUTPUT_PREFIX = "analysis/analysis."

def get_week_before_labor_day(year):
    # Find the first day of September
    first_september = date(year, 9, 1)
    # Find the first Monday in September
    labor_day = first_september + timedelta(days=(7 - first_september.weekday()) % 7)
    print(f"Labor Day in {year} is on {labor_day}")
    # Calculate the Saturday of the week before Labor Day
    saturday_before = labor_day - timedelta(days=9)
    # Generate the 9 days leading up to that Saturday
    dates = [saturday_before + timedelta(days=i) for i in range(10)]
    return dates

if __name__ == "__main__":
    import argparse
    import os
    import subprocess

    parser = argparse.ArgumentParser(description="Run analysis for a week before Labor Day.")
    parser.add_argument("-y", "--year", type=int, required=True, help="Year to analyze")
    args = parser.parse_args()

    # Get the dates for the week before Labor Day
    dates = get_week_before_labor_day(args.year)
    print (f"Dates for the week before Labor Day {args.year}: {dates}")
    # Create a directory for the analysis results
    os.makedirs("analysis", exist_ok=True)

    # Run the analysis for each date
    for date in dates:
        date_str = date.strftime("%Y/%m/%d")
        data_file = f"{INPUT_DIR}{date_str}"
        print(f"Running analysis for {date_str}...")
        outfile = f"{OUTPUT_PREFIX}"+ date_str.replace("/", ".") + ".out"

        with open(outfile, "w") as out:
            subprocess.run([ANALYZE_CMD + " " + data_file], check=True,
                           shell=True, stdout=out, stderr=out)
        print(f"Analysis for {date_str} completed.")
