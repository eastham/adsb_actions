#!/usr/bin/env python3
"""Stamp the landing page's headline stats from the map build being published.

examples/airbornehotspots/html/index_template.html carries three headline
numbers plus a date-range caption, each wrapped in <!--BEGIN:key-->...<!--END:key-->
markers. This script rewrites the text between those markers in place, leaving
the markers so it can run again next time.

The numbers must describe whatever map is live at the stable /v2/conus.html,
since that's the artifact the page's picture links to. That's why deploy_v2
calls this when it PROMOTES a map (--publish-as), not when one is merely built:
a throwaway test build must never rewrite the public page's claims. Both the
stats and the promoted .pmtiles/_quality.json come from one --source-stem, so
the page cannot drift from the map it describes.

Note the stats deliberately describe the published build, not everything under
data/v2/events — days outside the build aren't on the map, so counting them
would advertise data no visitor can see.

Usage (deploy_v2 does this for you; run it by hand after a copy edit):
    python src/tools/generate_index_stats.py --stem conus_20250601_20250831
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from hotspots.config import MAPS_DIR, REGIONAL_DIR
from hotspots.stage5_visualize import _parse_date_range_from_stem

_ROOT = Path(__file__).resolve().parent.parent.parent
PAGE = _ROOT / "examples" / "airbornehotspots" / "html" / "index_template.html"

# Mean ADS-B position reports per day inside the CONUS bbox, from exact line
# counts of three full days of grid shards (measured 2026-07-14):
#   20250605 = 35,372,561   20250712 = 34,459,582   20250820 = 37,620,798
# Spread is only ±4%, so scaling this by the build's day count is sound.
# Re-derive by counting lines in every data/v2/grid/<YYYYMMDD>/*.gz for a few
# days and taking the mean. One shard line == one position report.
POINTS_PER_DAY = 35_800_000


def _restamp(text: str, replacements: dict) -> str:
    """Replace marker bodies, keeping the <!--BEGIN:k-->/<!--END:k--> markers.

    Deliberately not generate_batch_outputs._render_template, which consumes its
    markers — right for a one-shot template render, but it would make this page
    stampable exactly once.
    """
    for key, value in replacements.items():
        pattern = f"(<!--BEGIN:{key}-->).*?(<!--END:{key}-->)"
        text, n = re.subn(pattern,
                          lambda m: m.group(1) + str(value) + m.group(2),
                          text, flags=re.DOTALL)
        if n == 0:
            raise KeyError(f"marker '{key}' not found in {PAGE.name}")
    return text


def _stem_days(stem: str) -> int | None:
    """Inclusive day count from a stem's trailing _YYYYMMDD_YYYYMMDD pair."""
    parts = stem.split("_")
    if len(parts) < 2:
        return None
    try:
        start = datetime.strptime(parts[-2], "%Y%m%d")
        end = datetime.strptime(parts[-1], "%Y%m%d")
    except ValueError:
        return None
    return (end - start).days + 1


def _humanize(points: int) -> str:
    """Render an extrapolated point count without implying false precision."""
    if points >= 1e9:
        return f"{points / 1e9:.1f} billion"
    return f"{points / 1e6:.0f} million"


def count_airports_with_coverage(quality_json: Path) -> int:
    """Airports the map draws with a colored (non-grey) icon.

    An airport greys out when it has no low-altitude tracks: zero tracks means
    no termination rate to score, which data_quality scores as "none". Test the
    track count rather than score == "none" — the count is the cause, the score
    is the effect, so this stays correct if a new score bucket ever appears.
    """
    quality = json.loads(quality_json.read_text())
    return sum(1 for q in quality.values()
               if (q.get("totalLowAltTracks") or 0) > 0)


def count_los_events(regional_parquet: Path) -> int:
    """Events the map shows by default, i.e. excluding low quality.

    The map hides low-quality events behind an opt-in checkbox, so counting them
    here would advertise dots a visitor can't see. These rows are deduped, unlike
    v1's per-airport counts which double-counted events near two airports.
    """
    import pandas as pd
    df = pd.read_parquet(regional_parquet, columns=["quality"])
    return int((df["quality"] != "low").sum())


def compute_headline_stats(stem: str, maps_dir: Path = MAPS_DIR,
                           regional_dir: Path = REGIONAL_DIR) -> dict:
    """Gather the landing page's headline stats for one published map stem.

    Each stat is independently guarded: a missing or unreadable input yields
    None for that stat only, so a partial build degrades to the page's existing
    numbers rather than crashing or stamping a zero.
    """
    date_range = _parse_date_range_from_stem(stem)
    if date_range is None:
        raise ValueError(
            f"cannot parse a _YYYYMMDD_YYYYMMDD date range from '{stem}' "
            "— the page's stats are scoped by the map's date range")
    start, end = date_range

    stats = {"date_range": f"{start} &ndash; {end}"}

    quality_json = Path(maps_dir) / f"{stem}_quality.json"
    try:
        stats["airports_with_data"] = f"{count_airports_with_coverage(quality_json):,}"
    except (OSError, ValueError) as e:
        print(f"WARNING: airport count unavailable ({quality_json}): {e}",
              file=sys.stderr)
        stats["airports_with_data"] = None

    regional_parquet = Path(regional_dir) / f"{stem}.parquet"
    try:
        stats["total_events"] = f"{count_los_events(regional_parquet):,}"
    except (OSError, ValueError, KeyError) as e:
        print(f"WARNING: event count unavailable ({regional_parquet}): {e}",
              file=sys.stderr)
        stats["total_events"] = None

    days = _stem_days(stem)
    stats["data_points"] = _humanize(POINTS_PER_DAY * days) if days else None

    return stats


def stamp_index_template(stats: dict, page: Path = PAGE,
                         dry_run: bool = False) -> str:
    """Rewrite the page's headline stats. Returns a one-line summary."""
    # Drop unavailable stats so their markers keep the page's current values.
    known = {k: v for k, v in stats.items() if v is not None}

    if not dry_run:
        page.write_text(_restamp(page.read_text(), known))

    return (f"{known.get('airports_with_data', '?')} airports / "
            f"{known.get('total_events', '?')} events / "
            f"{known.get('data_points', '?')} points / "
            f"{stats['date_range'].replace('&ndash;', '–')}")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stem", required=True,
                    help="Map stem being published, e.g. conus_20250601_20250831")
    ap.add_argument("--maps-dir", default=MAPS_DIR,
                    help="Directory holding <stem>_quality.json")
    ap.add_argument("--regional-dir", default=REGIONAL_DIR,
                    help="Directory holding <stem>.parquet")
    ap.add_argument("--dry-run", "-n", action="store_true",
                    help="Print what would be written without touching the page")
    args = ap.parse_args()

    try:
        stats = compute_headline_stats(args.stem, Path(args.maps_dir),
                                       Path(args.regional_dir))
        summary = stamp_index_template(stats, dry_run=args.dry_run)
    except (ValueError, KeyError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    verb = "would stamp" if args.dry_run else "stamped"
    print(f"{PAGE.name} {verb}: {summary}")
    if not args.dry_run:
        print("Commit the template, then: python src/tools/generate_batch_outputs.py ...")
        print("                           python src/tools/deploy_static")


if __name__ == "__main__":
    main()
