"""Source-data fetch for the v2 catch-up: ensure a day's CONUS_<mmddyy>.gz exists.

Thin wrapper around the v1-era download/extract/convert functions in
`src/tools/batch_los_pipeline.py` (the same sequence global_extractor.py's Phase 1
uses). It pulls the day's global tar parts from the adsb.lol GitHub releases,
untars, converts to a global sorted JSONL, then filters to a CONUS subset.

The v1 functions assume cwd == project root and hardcode DATA_DIR = Path("data").
`ensure_conus` therefore only supports conus_dir == <project_root>/data; it raises
if pointed elsewhere rather than silently writing to the wrong place.
"""

import sys
from datetime import datetime
from pathlib import Path

import requests

from hotspots import term

_ROOT = Path(__file__).resolve().parents[2]     # .../adsb_actions2
_TOOLS = _ROOT / "src" / "tools"
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))


def _release_published(date_obj: datetime) -> bool:
    """Is the adsb.lol globe_history release for this day published yet?

    HEADs the first tar part (both the prod-0 and prod-0tmp release tags) without
    downloading it. A leading-edge day whose release isn't up yet returns False so
    the catch-up can stop there instead of erroring."""
    date_iso = date_obj.strftime("%Y.%m.%d")
    year = date_obj.strftime("%Y")
    prefix = f"v{date_iso}-planes-readsb-prod-0"
    for tag in (prefix, prefix.replace("prod-0", "prod-0tmp")):
        url = (f"https://github.com/adsblol/globe_history_{year}"
               f"/releases/download/{tag}/{prefix}.tar.aa")
        try:
            # allow_redirects: GitHub release assets 302 to a CDN; a published
            # asset resolves to 200, an absent one to 404.
            r = requests.head(url, allow_redirects=True, timeout=30)
        except requests.RequestException:
            continue  # transient — let the real download surface a hard failure
        if r.status_code == 200:
            return True
    return False


def ensure_conus(date_obj: datetime, conus_dir: Path) -> Path | None:
    """Ensure data/CONUS_<mmddyy>.gz exists for date_obj, fetching from GitHub
    if needed.

    Returns the CONUS path, or None if the source release isn't published yet
    (normal at the leading edge — the caller stops there). Raises on a real
    failure (download/extract/convert error for a release that does exist).
    """
    # These import cwd-relative behavior (DATA_DIR=Path("data")), so keep the
    # imports local to make the coupling obvious and avoid import-time surprises.
    from batch_los_pipeline import (
        DATA_DIR, download_tar_parts, extract_traces,
        convert_traces_global, convert_global_to_conus,
    )

    conus_dir = Path(conus_dir)
    expected = (_ROOT / DATA_DIR).resolve()
    if conus_dir.resolve() != expected:
        raise ValueError(
            f"ensure_conus only supports conus_dir == {expected} "
            f"(the v1 fetch functions hardcode DATA_DIR); got {conus_dir}")

    date_compact = date_obj.strftime("%m%d%y")
    conus_gz = conus_dir / f"CONUS_{date_compact}.gz"
    if conus_gz.exists():
        print(term.ok(f"CONUS_{date_compact}.gz already present"))
        return conus_gz

    if not _release_published(date_obj):
        print(term.warn(
            f"source release for {date_obj:%Y-%m-%d} not published yet"))
        return None

    global_gz = conus_dir / f"global_{date_compact}.gz"
    if not global_gz.exists():
        print(term.stage(f"fetch {date_obj:%Y-%m-%d}: downloading tar parts"))
        if not download_tar_parts(date_obj, data_dir=str(conus_dir)):
            raise RuntimeError(
                f"tar download failed for {date_obj:%Y-%m-%d} "
                f"(release exists — likely a network/mount error)")
        print(term.stage(f"fetch {date_obj:%Y-%m-%d}: extracting traces"))
        if not extract_traces(date_obj):
            raise RuntimeError(f"trace extraction failed for {date_obj:%Y-%m-%d}")
        convert_traces_global(date_obj)

    print(term.stage(f"fetch {date_obj:%Y-%m-%d}: filtering to CONUS"))
    return convert_global_to_conus(date_obj)
