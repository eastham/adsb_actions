#!/usr/bin/env python3
"""
Process newly-requested airports from airbornehotspots.com/requests.jsonl.

Fetches the append-only JSONL request log, identifies airports not yet processed,
filters out unknown airport codes, and runs the batch LOS pipeline on them.
Non-CONUS airports are passed through — the pipeline's CONUS shard pass will
naturally produce empty output for them.

Designed to be called from a nightly cron job.

Usage:
    python src/tools/process_requests.py [--dry-run] [--days N] [--state-file PATH]
"""

import argparse
import json
import os
import sys
import subprocess
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

# Add src/tools to path for sibling imports
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import generate_airport_config
from batch_helpers import faa_to_icao, validate_date

REQUESTS_URL = "https://airbornehotspots.com/requests.jsonl"
DEFAULT_STATE_FILE = Path("airport_lists/processed_requests.json")
DEFAULT_DAYS = 30  # How many days back to analyze when no explicit date range given


def fetch_requests(url: str) -> list[dict]:
    """Download and parse the requests JSONL file."""
    print(f"Fetching requests from {url}...")
    req = urllib.request.Request(url, headers={"User-Agent": "airbornehotspots-pipeline/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        lines = resp.read().decode("utf-8").splitlines()
    entries = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError as e:
            print(f"  ⚠️  Skipping malformed line: {e}")
    print(f"  Fetched {len(entries)} entries.")
    return entries


def load_state(state_file: Path) -> dict:
    """Load processed-airports state. Returns dict with 'processed' set."""
    if state_file.exists():
        with open(state_file) as f:
            data = json.load(f)
        return data
    return {"processed": []}


def save_state(state_file: Path, state: dict) -> None:
    """Persist processed-airports state to disk."""
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2)
    print(f"State saved to {state_file}")


def airport_exists(icao: str) -> bool:
    """Return True if the airport code is known in the OurAirports database."""
    airport = generate_airport_config.load_airport(icao)
    if not airport and icao.startswith("K") and len(icao) == 4:
        airport = generate_airport_config.load_airport(icao[1:])
    if not airport:
        print(f"  ⚠️  {icao}: not found in airport database, skipping")
        return False
    return True


def get_new_airports(entries: list[dict], processed: set) -> list[str]:
    """Return ICAO codes from entries that haven't been processed yet.

    Deduplicates codes and normalizes to ICAO format. Preserves request order
    (first occurrence wins for deduplication).
    """
    seen = set()
    new_codes = []
    for entry in entries:
        raw = entry.get("airport", "").strip().upper()
        if not raw:
            continue
        icao = faa_to_icao(raw)
        if icao in processed:
            continue
        if icao in seen:
            continue
        seen.add(icao)
        new_codes.append(icao)
    return new_codes


def run_pipeline(airports: list[str], start_date: str, end_date: str,
                 dry_run: bool) -> int:
    """Run batch_los_pipeline.py for the given airports and date range.

    Writes a temporary airport list file and passes it to the pipeline.
    """
    tmp_list = Path("/tmp/process_requests_airports.txt")
    tmp_list.write_text("\n".join(airports) + "\n")
    print(f"  Airport list written to {tmp_list}")

    cmd = (
        f"source .venv/bin/activate && "
        f"python src/tools/batch_los_pipeline.py "
        f"--start-date {start_date} --end-date {end_date} "
        f"--airports {tmp_list}"
    )
    if dry_run:
        cmd += " --dry-run"

    print(f"Running pipeline: {cmd}")
    sys.stdout.flush()
    result = subprocess.run(cmd, shell=True, executable="/bin/bash")
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print actions without executing the pipeline")
    parser.add_argument("--start-date", type=validate_date, default=None,
                        help="Start date in mm/dd/yy format (default: today - N days)")
    parser.add_argument("--end-date", type=validate_date, default=None,
                        help="End date in mm/dd/yy format (default: today)")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS,
                        help=f"Days back to analyze when no --start-date given (default: {DEFAULT_DAYS})")
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE,
                        help=f"JSON file tracking processed airports (default: {DEFAULT_STATE_FILE})")
    parser.add_argument("--url", default=REQUESTS_URL,
                        help="URL of the requests JSONL file")
    args = parser.parse_args()

    # Load persisted state
    state = load_state(args.state_file)
    processed = set(state.get("processed", []))
    print(f"Previously processed airports: {len(processed)}")

    # Fetch remote requests
    try:
        entries = fetch_requests(args.url)
    except Exception as e:
        print(f"❌ Failed to fetch requests: {e}")
        sys.exit(1)

    # Find new airports not yet processed
    candidate_codes = get_new_airports(entries, processed)
    print(f"New (unprocessed) airport codes: {len(candidate_codes)}: "
          f"{', '.join(candidate_codes) if candidate_codes else '(none)'}")

    if not candidate_codes:
        print("Nothing to do.")
        return

    # Filter out unknown codes (non-CONUS airports pass through; the pipeline handles them)
    print("Validating airport codes...")
    valid_codes = [c for c in candidate_codes if airport_exists(c)]
    skipped = len(candidate_codes) - len(valid_codes)
    if skipped:
        print(f"  Skipped {skipped} unknown airport code(s)")

    # Skip airports that already have generated output
    base_dir = Path("examples/generated")
    already_done = [c for c in valid_codes if (base_dir / c / f"{c}_map.html").exists()]
    if already_done:
        print(f"  Already have output for: {', '.join(already_done)} — skipping")
        valid_codes = [c for c in valid_codes if c not in already_done]
    if not valid_codes:
        print("No valid airports to process.")
        # Still save state so we don't re-check these on next run
        state["processed"] = sorted(processed | set(candidate_codes))
        if not args.dry_run:
            save_state(args.state_file, state)
        return

    print(f"Airports to process: {', '.join(valid_codes)}")

    # Build date range
    end_dt = args.end_date or datetime.now()
    start_dt = args.start_date or (end_dt - timedelta(days=args.days - 1))
    start_date = start_dt.strftime("%m/%d/%y")
    end_date = end_dt.strftime("%m/%d/%y")
    print(f"Date range: {start_date} – {end_date}")

    # Run the pipeline
    rc = run_pipeline(valid_codes, start_date, end_date, dry_run=args.dry_run)
    if args.dry_run:
        return

    if rc != 0:
        print(f"❌ Pipeline exited with code {rc}")
        sys.exit(rc)

    # On success, mark all candidates (including non-CONUS) as processed so we
    # don't re-check them next run.
    state["processed"] = sorted(processed | set(candidate_codes))
    save_state(args.state_file, state)
    print(f"✅ Done. {len(valid_codes)} airport(s) processed.")


if __name__ == "__main__":
    main()
