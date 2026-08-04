#!/usr/bin/env python3
"""Audit examples/generated for stale and dead LOS event HTML files.

Stale: los_*.html with mtime before STALE_CUTOFF (Feb 23 2026, when auto-play
was added). These animations lack auto-play and their airports need regeneration.

Dead: los_*.html files present on disk but not linked from AIRPORT_map.html.
These are overflow animations excluded by the --max-los-events cap. They are
unreachable from any HTML and can be deleted.

Remote stale: with --check-remote, queries the rclone remote and reports
airports where the deployed files predate the cutoff (i.e. need redeployment).

Usage:
    python src/tools/audit_generated.py
    python src/tools/audit_generated.py --generated-dir /path/to/generated
    python src/tools/audit_generated.py --stale-cutoff 2026-03-01
    python src/tools/audit_generated.py --list-dead
    python src/tools/audit_generated.py --delete-dead
    python src/tools/audit_generated.py --delete-dead --confirm
    python src/tools/audit_generated.py --check-remote
    python src/tools/audit_generated.py --check-remote --remote myremote:bucket
"""

import argparse
import json
import os
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_REMOTE = "hotspots-initial:hotspots"
REMOTE_CHECK_WORKERS = 20  # parallel rclone calls for remote check

# Files generated before this date lack auto-play (added in Feb 23 2026 commit)
DEFAULT_STALE_CUTOFF = datetime(2026, 2, 23)

# los_TAIL1_TAIL2_YYYYMMDD_HHMMSS.html
LOS_FILENAME_RE = re.compile(r'^los_[^_]+_[^_]+_(\d{8})_\d{6}\.html$')


def parse_event_date(filename):
    """Extract event date from a los_*.html filename, or None if no match."""
    m = LOS_FILENAME_RE.match(filename)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), '%Y%m%d')
    except ValueError:
        return None


def get_linked_set(map_path):
    """Return set of los_*.html filenames linked from AIRPORT_map.html."""
    content = map_path.read_text(errors='replace')
    # visualizer.py generates: href='los_TAIL1_TAIL2_YYYYMMDD_HHMMSS.html'
    return set(re.findall(r"href='(los_[^']+)'", content))


def audit_airport(airport_dir, stale_cutoff_ts):
    """Audit one airport directory. Returns dict with stale and dead info."""
    icao = airport_dir.name
    map_file = airport_dir / f"{icao}_map.html"

    los_files = [f.name for f in airport_dir.iterdir()
                 if f.name.startswith('los_') and f.name.endswith('.html')]
    if not los_files:
        return {}

    # Stale: mtime before cutoff — no file content read needed
    stale_files = [f for f in los_files
                   if os.path.getmtime(airport_dir / f) < stale_cutoff_ts]
    stale_dates = sorted({d for f in stale_files
                          if (d := parse_event_date(f)) is not None})

    # Dead: on disk but not linked from map HTML
    # Unanimated: linked from map but missing auto_play in content
    if map_file.exists():
        linked = get_linked_set(map_file)
        dead_files = sorted(set(los_files) - linked)
        unanimated_files = sorted(
            f for f in linked
            if f in set(los_files)
            and 'auto_play' not in (airport_dir / f).read_text(errors='replace')
        )
    else:
        dead_files = sorted(los_files)
        unanimated_files = []

    unanimated_dates = sorted({d for f in unanimated_files
                                if (d := parse_event_date(f)) is not None})

    return {
        'icao': icao,
        'total_los': len(los_files),
        'stale_files': stale_files,
        'stale_dates': stale_dates,
        'dead_files': dead_files,
        'unanimated_files': unanimated_files,
        'unanimated_dates': unanimated_dates,
    }


def load_deploy_filter(icao, generated_dir):
    """Return set of animation filenames from the local deploy filter, or None if missing."""
    filter_path = generated_dir / icao / f"{icao}_deploy_filter.txt"
    if not filter_path.exists():
        return None
    kept = set()
    for line in filter_path.read_text().splitlines():
        line = line.strip()
        # Lines are like "+ los_....html" or "+ ICAO_map.html" or "- *"
        if line.startswith('+ los_') and line.endswith('.html'):
            kept.add(line[2:])  # strip leading '+ '
    return kept


def count_map_links(html_content):
    """Return the number of los_*.html hrefs in a map HTML string."""
    return len(re.findall(r"href='(los_[^']+)'", html_content))


# Matches "showing 300 of 926 events" or "showing 300 of 6,694 events" injected by
# visualizer when --max-los-events caps output (numbers may contain commas)
MAP_CAP_RE = re.compile(r'showing [\d,]+ of [\d,]+ events')

# Matches "+ N other date(s)" in the heatmap label — symptom of non-contiguous
# date coverage, typically caused by missing zero-event .csv.out files (now fixed).
MAP_NONCONTIGUOUS_RE = re.compile(r'\+ \d+ other dates?')


def check_noncontiguous_label(airport_dir):
    """Return the matched label snippet if the map has a non-contiguous date label, else None."""
    icao = airport_dir.name
    map_file = airport_dir / f"{icao}_map.html"
    if not map_file.exists():
        return None
    m = MAP_NONCONTIGUOUS_RE.search(map_file.read_text(errors='replace'))
    return m.group(0) if m else None


def check_remote_airport(icao, remote, generated_dir):
    """Query rclone for one airport's remote files. Returns (icao, result_dict, error).

    Detects two types of remote inconsistency:
    1. Stale: live files (in deploy filter) with mtime before DEFAULT_STALE_CUTOFF.
    2. Overcrowded map: the remote _map.html links to more events than the local one,
       meaning the remote was deployed before the 300-event cap was applied.

    Returns (icao, None, error_str) on rclone failure.
    """
    result = subprocess.run(
        ["rclone", "lsjson", f"{remote}/{icao}/"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        return icao, None, result.stderr.strip()

    try:
        entries = json.loads(result.stdout)
    except json.JSONDecodeError as e:
        return icao, None, str(e)

    # If a deploy filter exists locally, only consider files it lists as "live"
    live_files = load_deploy_filter(icao, generated_dir)

    cutoff = DEFAULT_STALE_CUTOFF.replace(tzinfo=timezone.utc)
    stale_dates = set()
    for entry in entries:
        name = entry.get("Path", "")
        if not (name.startswith("los_") and name.endswith(".html")):
            continue
        # Skip if not a live (map-linked) file
        if live_files is not None and name not in live_files:
            continue
        mod_str = entry.get("ModTime", "")
        try:
            # ModTime is RFC3339 with sub-second precision
            mod = datetime.fromisoformat(mod_str).astimezone(timezone.utc)
        except ValueError:
            continue
        if mod < cutoff:
            d = parse_event_date(name)
            if d:
                stale_dates.add(d)

    # Detect overcrowded remote map: local map has the "showing N of M events" cap
    # notice but the remote map doesn't, meaning remote was deployed before the cap.
    overcrowded = False
    remote_map_link_count = None
    local_map_path = generated_dir / icao / f"{icao}_map.html"
    if local_map_path.exists():
        local_html = local_map_path.read_text(errors='replace')
        local_has_cap = bool(MAP_CAP_RE.search(local_html))
        if local_has_cap:
            cat = subprocess.run(
                ["rclone", "cat", f"{remote}/{icao}/{icao}_map.html"],
                capture_output=True, text=True
            )
            if cat.returncode == 0:
                remote_map_link_count = count_map_links(cat.stdout)
                remote_has_cap = bool(MAP_CAP_RE.search(cat.stdout))
                overcrowded = not remote_has_cap

    return icao, {
        'stale_dates': sorted(stale_dates),
        'overcrowded': overcrowded,
        'remote_map_link_count': remote_map_link_count,
    }, None


def format_rerun_command(icao, dates):
    """Format a batch_los_pipeline rerun command for the given event date range."""
    start = dates[0].strftime('%m/%d/%y')
    end = dates[-1].strftime('%m/%d/%y')
    return (f"python src/tools/batch_los_pipeline.py "
            f"--airports {icao} "
            f"--start-date {start} --end-date {end} "
            f"--analysis-only")


def run_remote_check(airport_dirs, stale_cutoff, remote, generated_dir):
    """Check remote for stale event files across all airports in parallel."""
    print(f"Checking remote {remote} ({len(airport_dirs)} airports, "
          f"{REMOTE_CHECK_WORKERS} parallel)...", flush=True)

    icao_list = [d.name for d in airport_dirs]
    remote_stale = []
    remote_overcrowded = []
    errors = []

    with ThreadPoolExecutor(max_workers=REMOTE_CHECK_WORKERS) as pool:
        futures = {pool.submit(check_remote_airport, icao, remote, generated_dir): icao
                   for icao in icao_list}
        done = 0
        for fut in as_completed(futures):
            done += 1
            if done % 50 == 0:
                print(f"  {done}/{len(icao_list)}...", flush=True)
            icao, info, err = fut.result()
            if err:
                errors.append((icao, err))
            elif info:
                if info['stale_dates']:
                    remote_stale.append({'icao': icao, 'stale_dates': info['stale_dates']})
                if info['overcrowded']:
                    remote_overcrowded.append({
                        'icao': icao,
                        'remote_map_link_count': info['remote_map_link_count'],
                    })

    remote_stale.sort(key=lambda x: x['icao'])

    print()
    print("=" * 60)
    print(f"REMOTE STALE EVENT FILES  (ModTime before {stale_cutoff.date()})")
    if remote_stale:
        print(f"  {len(remote_stale)} airport(s) need redeployment")
        print("=" * 60)
        for r in remote_stale:
            dates = r['stale_dates']
            start = dates[0].strftime('%m/%d/%y')
            end = dates[-1].strftime('%m/%d/%y')
            print(f"\n  {r['icao']}: stale events {start} to {end}")
            print(f"  Redeploy: python src/tools/deploy_airports "
                  f"<airports_file_containing_{r['icao']}>  "
                  f"# after rerunning locally")
    else:
        print("  None found.")

    remote_overcrowded.sort(key=lambda x: x['icao'])
    print()
    print("=" * 60)
    print("REMOTE OVERCROWDED MAP  (remote _map.html links more events than local)")
    if remote_overcrowded:
        print(f"  {len(remote_overcrowded)} airport(s) deployed before 300-event cap")
        print("=" * 60)
        for r in remote_overcrowded:
            print(f"\n  {r['icao']}: remote map links {r['remote_map_link_count']} events (local: 300)")
            print(f"  Redeploy: python src/tools/deploy_airports "
                  f"<airports_file_containing_{r['icao']}>")
    else:
        print("  None found.")

    if errors:
        print(f"\n  Errors ({len(errors)} airports):")
        for icao, err in errors[:10]:
            print(f"    {icao}: {err}")
        if len(errors) > 10:
            print(f"    ... and {len(errors) - 10} more")

    return remote_stale, remote_overcrowded


def run_audit(generated_dir, stale_cutoff, list_dead=False,
              delete_dead=False, confirm=False, check_remote=False,
              remote=DEFAULT_REMOTE):
    cutoff_ts = stale_cutoff.timestamp()

    airport_dirs = sorted(
        d for d in generated_dir.iterdir()
        if d.is_dir() and d.name != 'tiles'
    )

    stale_airports = []
    dead_airports = []
    unanimated_airports = []
    noncontiguous_airports = []
    total_stale = 0
    total_dead = 0
    total_unanimated = 0

    for i, adir in enumerate(airport_dirs, 1):
        print(f"  [{i}/{len(airport_dirs)}] {adir.name}...", end='\r', flush=True)
        result = audit_airport(adir, cutoff_ts)
        if not result:
            continue
        if result['stale_dates']:
            stale_airports.append(result)
            total_stale += len(result['stale_files'])
        if result['dead_files']:
            dead_airports.append(result)
            total_dead += len(result['dead_files'])
        if result['unanimated_dates']:
            unanimated_airports.append(result)
            total_unanimated += len(result['unanimated_files'])
        label_snippet = check_noncontiguous_label(adir)
        if label_snippet:
            noncontiguous_airports.append((adir.name, label_snippet))
    print(f"  Scanned {len(airport_dirs)} airports.        ")

    # --- Stale report ---
    print("=" * 60)
    print(f"STALE EVENT FILES  (mtime before {stale_cutoff.date()})")
    if stale_airports:
        print(f"  {len(stale_airports)} airport(s), {total_stale:,} files need regeneration")
        print("=" * 60)
        for r in sorted(stale_airports, key=lambda x: x['icao']):
            dates = r['stale_dates']
            start = dates[0].strftime('%m/%d/%y')
            end = dates[-1].strftime('%m/%d/%y')
            print(f"\n  {r['icao']}: {len(r['stale_files'])} stale files "
                  f"spanning {len(dates)} event dates ({start} to {end})")
            print(f"  Rerun: {format_rerun_command(r['icao'], dates)}")
    else:
        print("  None found.")

    # --- Dead report ---
    print()
    print("=" * 60)
    print(f"DEAD EVENT FILES  (present on disk, not linked from map)")
    if dead_airports:
        print(f"  {len(dead_airports)} airport(s), {total_dead:,} files can be deleted")
        print("=" * 60)
        for r in sorted(dead_airports, key=lambda x: x['icao']):
            pct = round(100 * len(r['dead_files']) / r['total_los']) if r['total_los'] else 0
            print(f"  {r['icao']}: {len(r['dead_files'])} dead / {r['total_los']} total ({pct}%)")
            if list_dead:
                for f in r['dead_files']:
                    print(f"    {f}")
    else:
        print("  None found.")

    # --- Unanimated report ---
    print()
    print("=" * 60)
    print("UNANIMATED EVENT FILES  (linked from map, missing auto_play)")
    if unanimated_airports:
        print(f"  {len(unanimated_airports)} airport(s), {total_unanimated:,} files need regeneration")
        print("=" * 60)
        for r in sorted(unanimated_airports, key=lambda x: x['icao']):
            dates = r['unanimated_dates']
            start = dates[0].strftime('%m/%d/%y')
            end = dates[-1].strftime('%m/%d/%y')
            print(f"\n  {r['icao']}: {len(r['unanimated_files'])} unanimated files "
                  f"spanning {len(dates)} event dates ({start} to {end})")
            print(f"  Rerun: {format_rerun_command(r['icao'], dates)}")
    else:
        print("  None found.")

    # --- Non-contiguous date label report ---
    print()
    print("=" * 60)
    print("NON-CONTIGUOUS DATE LABELS  (map shows '+ N other date(s)')")
    print("  Caused by missing zero-event .csv.out files; rerun analysis to fix.")
    if noncontiguous_airports:
        print(f"  {len(noncontiguous_airports)} airport(s) affected")
        print("=" * 60)
        for icao, snippet in sorted(noncontiguous_airports):
            print(f"  {icao}: {snippet}")
    else:
        print("  None found.")

    # --- Optional deletion ---
    if delete_dead and dead_airports:
        print()
        if not confirm:
            print(f"DRY RUN: would delete {total_dead:,} dead files. "
                  f"Add --confirm to actually delete.")
        else:
            print(f"Deleting {total_dead:,} dead files...")
            deleted = 0
            for r in dead_airports:
                adir = generated_dir / r['icao']
                for f in r['dead_files']:
                    try:
                        (adir / f).unlink()
                        deleted += 1
                    except OSError as e:
                        print(f"  Error deleting {r['icao']}/{f}: {e}", file=sys.stderr)
            print(f"Deleted {deleted:,} files.")

    # --- Remote check ---
    remote_stale = []
    remote_overcrowded = []
    if check_remote:
        remote_stale, remote_overcrowded = run_remote_check(
            airport_dirs, stale_cutoff, remote, generated_dir)

    # --- Summary ---
    print()
    print("SUMMARY")
    print(f"  {len(airport_dirs)} airports scanned")
    print(f"  Stale (local):      {total_stale:,} files across {len(stale_airports)} airports")
    print(f"  Unanimated (local): {total_unanimated:,} files across {len(unanimated_airports)} airports")
    print(f"  Dead (local):       {total_dead:,} files across {len(dead_airports)} airports")
    print(f"  Non-contiguous date label: {len(noncontiguous_airports)} airports need rerun")
    if check_remote:
        print(f"  Stale (remote): {len(remote_stale)} airports need redeployment")
        print(f"  Overcrowded (remote): {len(remote_overcrowded)} airports deployed before 300-event cap")


def main():
    parser = argparse.ArgumentParser(
        description="Audit examples/generated for stale and dead LOS event HTML files"
    )
    parser.add_argument("--generated-dir", type=Path, default=Path("examples/generated"),
                        help="Path to generated directory (default: examples/generated)")
    parser.add_argument("--stale-cutoff", default=DEFAULT_STALE_CUTOFF.strftime('%Y-%m-%d'),
                        help=f"Mtime cutoff for stale detection (default: {DEFAULT_STALE_CUTOFF.date()})")
    parser.add_argument("--list-dead", action="store_true",
                        help="Print individual filenames of dead files")
    parser.add_argument("--delete-dead", action="store_true",
                        help="Delete dead files (dry-run unless --confirm is passed)")
    parser.add_argument("--confirm", action="store_true",
                        help="Actually perform deletion (required with --delete-dead)")
    parser.add_argument("--check-remote", action="store_true",
                        help="Query rclone remote for stale deployed files")
    parser.add_argument("--remote", default=DEFAULT_REMOTE,
                        help=f"rclone remote to check (default: {DEFAULT_REMOTE})")
    args = parser.parse_args()

    try:
        cutoff = datetime.strptime(args.stale_cutoff, '%Y-%m-%d')
    except ValueError:
        print(f"Error: invalid --stale-cutoff '{args.stale_cutoff}'. Use YYYY-MM-DD.",
              file=sys.stderr)
        sys.exit(1)

    if not args.generated_dir.is_dir():
        print(f"Error: not a directory: {args.generated_dir}", file=sys.stderr)
        sys.exit(1)

    run_audit(args.generated_dir, cutoff,
              list_dead=args.list_dead,
              delete_dead=args.delete_dead,
              confirm=args.confirm,
              check_remote=args.check_remote,
              remote=args.remote)


if __name__ == "__main__":
    main()
