#!/usr/bin/env python3
"""Delete .gz files listed in a bad-data text file."""

import argparse
import glob
import os
import re
import sys


def extract_gz_paths(line):
    """Extract all *.gz file paths from a log line."""
    return re.findall(r'[\w./-]+\.gz', line)


def main():
    parser = argparse.ArgumentParser(
        description="Delete .gz files referenced in a text file"
    )
    parser.add_argument("bad_data_file", nargs="?", default="bad_data.txt",
                        help="Text file containing .gz paths (default: bad_data.txt)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print files that would be deleted without deleting")
    args = parser.parse_args()

    if not os.path.exists(args.bad_data_file):
        print(f"File not found: {args.bad_data_file}", file=sys.stderr)
        sys.exit(1)

    paths = set()
    with open(args.bad_data_file) as f:
        for line in f:
            paths.update(extract_gz_paths(line))

    deleted = 0
    missing = 0
    for path in sorted(paths):
        if os.path.exists(path):
            if args.dry_run:
                print(f"Would delete: {path}")
            else:
                os.remove(path)
                print(f"Deleted: {path}")
            deleted += 1
        else:
            missing += 1

    action = "Would delete" if args.dry_run else "Deleted"
    print(f"\n{action} {deleted} files, {missing} not found")


if __name__ == "__main__":
    main()
