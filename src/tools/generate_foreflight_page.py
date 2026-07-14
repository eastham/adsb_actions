#!/usr/bin/env python3
"""Stamp the public ForeFlight page with the live pack's date range and size.

examples/airbornehotspots/html/foreflight.html is an ordinary static page — the
site nav links to it and deploy_static ships it like any other. The only thing
special about it is that two values are wrapped in <!--BEGIN:key-->...<!--END:key-->
comment markers; this script rewrites the text between those markers in place,
leaving the markers so it can run again next time.

The range shown must describe whatever pack is live at the stable
/foreflight/conus.zip, since that's what the page's download buttons hand out.
That's why deploy_v2 calls this when it PROMOTES a pack (--foreflight-publish-as),
not when one is merely built: a throwaway test build must never rewrite the
public page's claims.

Usage (deploy_v2 does this for you; run it by hand after a copy edit):
    python src/tools/generate_foreflight_page.py \
        --pack data/v2/foreflight/conus_20250601_20250831.zip
"""

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from hotspots.stage5_visualize import _parse_date_range_from_stem

_ROOT = Path(__file__).resolve().parent.parent.parent
PAGE = _ROOT / "examples" / "airbornehotspots" / "html" / "foreflight.html"


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


def update_foreflight_page(pack_zip, page: Path = PAGE, dry_run: bool = False) -> str:
    """Rewrite the page's date range and pack size from the pack .zip.

    Returns a one-line summary of what was (or would be) written.
    """
    pack_zip = Path(pack_zip)
    date_range = _parse_date_range_from_stem(pack_zip.stem)
    if date_range is None:
        raise ValueError(
            f"cannot parse a _YYYYMMDD_YYYYMMDD date range from '{pack_zip.stem}' "
            "— the page's data range comes from the pack filename")
    start, end = date_range
    size = f"{pack_zip.stat().st_size / 1e6:.0f} MB"

    if not dry_run:
        page.write_text(_restamp(page.read_text(), {
            "date_range": f"{start} &ndash; {end}",
            "pack_size": size,
        }))
    return f"{start} – {end}, {size}"


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pack", required=True, metavar="ZIP",
                    help="ForeFlight pack .zip, named <region>_<YYYYMMDD>_<YYYYMMDD>.zip")
    ap.add_argument("--dry-run", "-n", action="store_true",
                    help="Print what would be written without touching the page")
    args = ap.parse_args()

    pack = Path(args.pack)
    if not pack.is_file():
        print(f"ERROR: pack not found: {pack}", file=sys.stderr)
        sys.exit(1)

    try:
        summary = update_foreflight_page(pack, dry_run=args.dry_run)
    except (ValueError, KeyError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    verb = "would stamp" if args.dry_run else "stamped"
    print(f"{PAGE.name} {verb}: {summary}")
    if not args.dry_run:
        print("Commit the page, then: python src/tools/deploy_static")


if __name__ == "__main__":
    main()
