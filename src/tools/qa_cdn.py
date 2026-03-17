#!/usr/bin/env python3
"""
qa_cdn.py - QA check for the live airbornehotspots.com CDN.

Two-pass check:
  Source A: Every airport listed in the live index.html is fully validated.
  Source B: A random sample of airports from site_manifest.yaml (those NOT in the index)
            are probed to detect airports that were deployed but missed in index.html.

Example:
    python src/tools/qa_cdn.py
    python src/tools/qa_cdn.py --sample-pct 50
    python src/tools/qa_cdn.py --airports KWVI,KATL --verbose
    python src/tools/qa_cdn.py --dry-run
"""

import argparse
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import yaml

sys.path.insert(0, "src/tools")
from batch_helpers import faa_to_icao

BASE_URL = "https://airbornehotspots.com"
DEFAULT_MANIFEST = "airport_lists/site_manifest.yaml"
DEFAULT_SAMPLE_PCT = 20
DEFAULT_SAMPLE_EVENTS = 20
DEFAULT_WORKERS = 10

ROOT_FILES = [
    "index.html",
    "unavailable.html",
    "methodology.html",
    "faq.html",
    "contact.html",
    "accessibility.html",
    "hotspot.png",
]

# --- helpers ---

def get(url, timeout=15):
    """GET with simple retry on transient errors."""
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=timeout)
            return r
        except requests.RequestException as e:
            if attempt == 2:
                raise
            time.sleep(1)

def head(url, timeout=10):
    for attempt in range(3):
        try:
            r = requests.head(url, timeout=timeout, allow_redirects=True)
            return r
        except requests.RequestException as e:
            if attempt == 2:
                raise
            time.sleep(1)

def load_airport_list(path):
    """Load FAA/ICAO codes from a list file, skipping comments and blanks."""
    codes = []
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                codes.append(line.split()[0])  # strip trailing whitespace
    except FileNotFoundError:
        print(f"  WARNING: airport list not found: {path}", file=sys.stderr)
    return codes

def load_manifest_airports(manifest_path):
    """Load all airports from the lists referenced in site_manifest.yaml."""
    with open(manifest_path) as f:
        manifest = yaml.safe_load(f)
    all_icao = set()
    for list_path in manifest.get("airport_lists", []):
        for faa in load_airport_list(list_path):
            all_icao.add(faa_to_icao(faa))
    return all_icao

def parse_index_airports(html):
    """Extract ICAO codes from href links like KWVI/KWVI_map.html."""
    pattern = re.compile(r'href=["\'](\w+)/\1_map\.html["\']')
    return sorted(set(m.group(1) for m in pattern.finditer(html)))

# --- per-airport checks ---

# Patterns to extract info inlined into map.html by the generator
_RE_EVENT_HREF = re.compile(r"href='(los_[^']+\.html)'")
_RE_QUALITY_LABEL = re.compile(r'qualityData\s*=\s*\{[^}]*"label":\s*"([^"]+)"')


def check_airport(icao, sample_events, verbose):
    """
    Fetch the airport's map.html, validate it, extract inlined stats and event links,
    then HEAD-check a sample of event HTMLs.
    Returns (icao, status, details_list, info_dict).
    """
    issues = []
    warnings = []
    verbose_notes = []  # shown only in verbose mode, not counted as warnings
    info = {"events_per_day": None, "total_events": None, "quality": None}

    # 1. Fetch map.html (need full content to extract event links and stats)
    map_url = f"{BASE_URL}/{icao}/{icao}_map.html"
    map_text = None
    try:
        r = get(map_url)
        if r.status_code != 200:
            issues.append(f"map.html HTTP {r.status_code}")
        else:
            map_text = r.text
    except Exception as e:
        issues.append(f"map.html error: {e}")

    event_htmls = []
    if map_text:
        m = _RE_QUALITY_LABEL.search(map_text)
        if m:
            info["quality"] = m.group(1)

        # Extract event HTML links (deduplicated, preserving first-seen order)
        seen = set()
        for fname in _RE_EVENT_HREF.findall(map_text):
            if fname not in seen:
                seen.add(fname)
                event_htmls.append(fname)

        # Use event link count as a proxy for total events shown on the map
        info["total_events"] = len(event_htmls)

        if not event_htmls:
            # Zero events is valid (airport processed but had no qualifying LOS events)
            verbose_notes.append("no event links (zero LOS events found for this airport)")

    # 2. Sample event HTMLs
    sample = random.sample(event_htmls, min(sample_events, len(event_htmls)))
    for fname in sample:
        evt_url = f"{BASE_URL}/{icao}/{fname}"
        try:
            r = head(evt_url)
            if r.status_code != 200:
                warnings.append(f"event {fname} HTTP {r.status_code}")
            else:
                verbose_notes.append(f"event {fname} OK")
        except Exception as e:
            warnings.append(f"event {fname} error: {e}")

    if issues:
        status = "FAIL"
    elif warnings:
        status = "WARN"
    else:
        status = "OK"

    details = issues + warnings + (verbose_notes if verbose else [])
    return icao, status, details, info


# --- main ---

def main():
    global BASE_URL
    parser = argparse.ArgumentParser(
        description="QA check for the live airbornehotspots.com CDN.")
    parser.add_argument("--base-url", default=BASE_URL,
                        help=f"CDN base URL (default: {BASE_URL})")
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST,
                        help=f"YAML manifest of all intended airports (default: {DEFAULT_MANIFEST})")
    parser.add_argument("--sample-pct", type=float, default=DEFAULT_SAMPLE_PCT,
                        help=f"Percent of manifest-only airports to probe (default: {DEFAULT_SAMPLE_PCT})")
    parser.add_argument("--sample-events", type=int, default=DEFAULT_SAMPLE_EVENTS,
                        help=f"Number of event HTMLs to sample per airport (default: {DEFAULT_SAMPLE_EVENTS})")
    parser.add_argument("--airports", default=None,
                        help="Comma-separated ICAO codes to check instead of auto-discovery")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Parallel workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show per-airport details even when OK")
    parser.add_argument("--skip-index", action="store_true",
                        help="Skip index airport checks; only run manifest sampling")
    parser.add_argument("--dry-run", "-n", action="store_true",
                        help="Show what would be checked without making requests")
    args = parser.parse_args()

    BASE_URL = args.base_url.rstrip("/")

    # --- Step 1: get index airports ---
    if args.airports:
        index_airports = [faa_to_icao(a.strip()) for a in args.airports.split(",")]
        manifest_only = []
        print(f"Manual airport list: {index_airports}")
    else:
        print(f"Fetching index: {BASE_URL}/index.html ...", flush=True)
        if not args.dry_run:
            try:
                r = get(f"{BASE_URL}/index.html")
                r.raise_for_status()
                index_airports = parse_index_airports(r.text)
            except Exception as e:
                print(f"FATAL: cannot fetch index.html: {e}", file=sys.stderr)
                sys.exit(1)
        else:
            index_airports = ["(dry-run)"]

        print(f"  Found {len(index_airports)} airports in index.html")

        # --- Step 2: manifest airports not in index ---
        try:
            manifest_icao = load_manifest_airports(args.manifest)
        except FileNotFoundError:
            print(f"WARNING: manifest not found: {args.manifest}", file=sys.stderr)
            manifest_icao = set()

        index_set = set(index_airports)
        manifest_only_all = sorted(manifest_icao - index_set)
        n_sample = max(1, int(len(manifest_only_all) * args.sample_pct / 100))
        manifest_only = random.sample(manifest_only_all, min(n_sample, len(manifest_only_all)))
        print(f"  Manifest has {len(manifest_icao)} airports; "
              f"{len(manifest_only_all)} not in index; "
              f"sampling {len(manifest_only)} ({args.sample_pct:.0f}%)")

    # --- Step 3: root file checks ---
    print(f"\nChecking root files ...", flush=True)
    root_failures = []
    if not args.dry_run:
        for fname in ROOT_FILES:
            url = f"{BASE_URL}/{fname}"
            try:
                r = head(url)
                status = "OK" if r.status_code == 200 else f"HTTP {r.status_code}"
            except Exception as e:
                status = f"ERROR: {e}"
            symbol = "✓" if status == "OK" else "✗"
            if args.verbose or status != "OK":
                print(f"  {symbol} {fname}: {status}")
            if status != "OK":
                root_failures.append(fname)
    if not root_failures and not args.verbose:
        print(f"  All {len(ROOT_FILES)} root files OK")

    # --- Step 4: index airport checks ---
    if args.skip_index:
        print(f"\nSkipping {len(index_airports)} index airports (--skip-index)")
    else:
        print(f"\nChecking {len(index_airports)} index airports ...", flush=True)
    if not args.skip_index and args.dry_run:
        for icao in index_airports[:5]:
            print(f"  Would check: {icao}")
        if len(index_airports) > 5:
            print(f"  ... and {len(index_airports) - 5} more")
    elif not args.skip_index:
        results = {}
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(check_airport, icao, args.sample_events, args.verbose): icao
                for icao in index_airports
            }
            for i, future in enumerate(as_completed(futures), 1):
                icao = futures[future]
                try:
                    icao, status, details, info = future.result()
                except Exception as e:
                    icao = futures[future]
                    status, details, info = "FAIL", [str(e)], {}
                results[icao] = (status, details, info)
                if args.verbose or status != "OK":
                    nevents = f"{info['total_events']} events" if info.get("total_events") is not None else ""
                    qual = info.get("quality") or ""
                    detail_str = "; ".join(details) if details else ""
                    print(f"  [{status:4s}] {icao:6s}  {qual:10s} {nevents:12s} {detail_str}")
                elif i % 20 == 0:
                    print(f"  ... {i}/{len(index_airports)} checked", flush=True)

        ok = sum(1 for s, _, _ in results.values() if s == "OK")
        warn = sum(1 for s, _, _ in results.values() if s == "WARN")
        fail = sum(1 for s, _, _ in results.values() if s == "FAIL")
        print(f"\n  Index airports: {ok} OK, {warn} WARN, {fail} FAIL")

        if not args.verbose:
            failed = [(icao, details) for icao, (s, details, _) in results.items() if s != "OK"]
            for icao, details in sorted(failed):
                print(f"  [{results[icao][0]:4s}] {icao}: {'; '.join(details)}")

    # --- Step 5: manifest-only sample checks (full validation, same as index airports) ---
    manifest_results = {}
    if manifest_only and not args.dry_run:
        print(f"\nChecking {len(manifest_only)} manifest-only airports: {', '.join(sorted(manifest_only))} ...", flush=True)
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(check_airport, icao, args.sample_events, args.verbose): icao
                for icao in manifest_only
            }
            for future in as_completed(futures):
                icao = futures[future]
                try:
                    icao, status, details, info = future.result()
                except Exception as e:
                    status, details, info = "FAIL", [str(e)], {}
                manifest_results[icao] = (status, details, info)
                # ABSENT = not deployed yet (map.html 404), which is fine
                if status == "FAIL" and info.get("total_events") is None and not details:
                    status = "ABSENT"
                if args.verbose or status not in ("OK", "ABSENT"):
                    nevents = f"{info['total_events']} events" if info.get("total_events") is not None else ""
                    qual = info.get("quality") or ""
                    detail_str = "; ".join(details) if details else ""
                    print(f"  [{status:4s}] {icao:6s}  {qual:10s} {nevents:12s} {detail_str}")

        absent = sum(1 for s, d, _ in manifest_results.values()
                     if s == "FAIL" and any("HTTP 404" in x for x in d))
        ok = sum(1 for s, _, _ in manifest_results.values() if s == "OK")
        warn = sum(1 for s, _, _ in manifest_results.values() if s == "WARN")
        fail = sum(1 for s, d, _ in manifest_results.values()
                   if s == "FAIL" and not any("HTTP 404" in x for x in d))
        print(f"  {ok} OK, {warn} WARN, {fail} FAIL, {absent} absent (not yet deployed)")
    elif manifest_only and args.dry_run:
        print(f"\nWould check {len(manifest_only)} manifest-only airports")

    # --- exit code ---
    if not args.dry_run:
        failures = list(root_failures)
        try:
            failures += [icao for icao, (s, _, _) in results.items() if s == "FAIL"]
        except NameError:
            pass
        # manifest airports that exist on CDN but have issues (skip purely-absent ones)
        failures += [icao for icao, (s, d, _) in manifest_results.items()
                     if s == "FAIL" and not any("HTTP 404" in x for x in d)]
        if failures:
            print(f"\nFAIL: {len(failures)} issue(s) found")
            sys.exit(1)
        else:
            print("\nAll checks passed.")

if __name__ == "__main__":
    main()
