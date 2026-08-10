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
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import requests

from hotspots import term


@dataclass
class FetchResult:
    """One day's fetch outcome: "ok" (conus set), "missing" (unpublished — a gap
    or the leading edge), or "corrupt" (tar short or unextractable after a retry;
    skip it)."""
    status: str            # "ok" | "missing" | "corrupt"
    conus: Path | None = None
    detail: str = ""       # human-readable reason for missing/corrupt


_ROOT = Path(__file__).resolve().parents[2]     # .../adsb_actions2
_TOOLS = _ROOT / "src" / "tools"
if str(_TOOLS) not in sys.path:
    sys.path.insert(0, str(_TOOLS))


def _remote_part_size(date_obj: datetime, ext: str) -> int | None:
    """Content-Length the release advertises for one tar part (.tar.aa/.tar.ab),
    or None if neither release tag has it. Tries prod-0 then prod-0tmp, matching
    how download_tar_parts resolves the URL."""
    date_iso = date_obj.strftime("%Y.%m.%d")
    year = date_obj.strftime("%Y")
    prefix = f"v{date_iso}-planes-readsb-prod-0"
    for tag in (prefix, prefix.replace("prod-0", "prod-0tmp")):
        url = (f"https://github.com/adsblol/globe_history_{year}"
               f"/releases/download/{tag}/{prefix}.tar.{ext}")
        try:
            r = requests.head(url, allow_redirects=True, timeout=30)
        except requests.RequestException:
            continue
        if r.status_code == 200 and r.headers.get("content-length"):
            return int(r.headers["content-length"])
    return None


def _part_exts(date_obj: datetime, data_dir: Path) -> list[str]:
    """The tar-part suffixes on disk for this day, in order (["aa","ab",...])."""
    date_iso = date_obj.strftime("%Y.%m.%d")
    prefix = f"v{date_iso}-planes-readsb-prod-0"
    return sorted(p.suffix.lstrip(".")
                  for p in data_dir.glob(f"{prefix}.tar.a*"))


def _tar_parts_ok(date_obj: datetime, data_dir: Path) -> bool:
    """Do we have EVERY tar part the release offers, each full-length?

    Two failure modes, both silent to the extract's `tar` (it treats any early
    cut-off as EOF and exits 0 on partial data):
      1. a present part is short (aborted download);
      2. a whole trailing part is missing (high-volume days split into 3+ parts —
         aa/ab/ac — and only aa/ab got fetched, dropping the archive tail).
    We check by size — each on-disk part vs its Content-Length — AND confirm the
    part after the last on-disk one genuinely 404s on the server, so a missing
    tail is caught. Fails closed (absent/short/size-unknown/more-on-server → False)
    so a suspect tar forces a fresh re-download instead of being trusted."""
    date_iso = date_obj.strftime("%Y.%m.%d")
    prefix = f"v{date_iso}-planes-readsb-prod-0"
    exts = _part_exts(date_obj, data_dir)
    if not exts:
        return False
    for ext in exts:
        local = data_dir / f"{prefix}.tar.{ext}"
        expected = _remote_part_size(date_obj, ext)
        if expected is None:
            print(term.warn(f"  could not confirm expected size of {local.name} "
                            f"— treating as incomplete"))
            return False
        actual = local.stat().st_size
        if actual != expected:
            print(term.warn(f"  {local.name} is {actual:,}B, expected "
                            f"{expected:,}B — incomplete"))
            return False
    # Is there another part on the server past our last one? If so we're missing
    # the tail (the June-24-style 3-part bug).
    nxt = "a" + chr(ord(exts[-1][1]) + 1)      # aa->ab, ab->ac, ...
    if _remote_part_size(date_obj, nxt) is not None:
        print(term.warn(f"  server has a further part .{nxt} we didn't fetch "
                        f"— tar tail missing"))
        return False
    return True


def _remove_tar_parts(date_obj: datetime, data_dir: Path) -> None:
    """Delete the day's downloaded tar parts so the next fetch re-downloads fresh."""
    date_iso = date_obj.strftime("%Y.%m.%d")
    prefix = f"v{date_iso}-planes-readsb-prod-0"
    for p in data_dir.glob(f"{prefix}.tar.a*"):
        print(term.warn(f"  removing corrupt/partial {p.name}"))
        p.unlink(missing_ok=True)


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


def ensure_conus(date_obj: datetime, conus_dir: Path) -> FetchResult:
    """Ensure data/CONUS_<mmddyy>.gz exists for date_obj, fetching from GitHub if
    needed. Returns a FetchResult (ok / missing / corrupt — see that class); a bad
    tar (short download or unextractable archive) yields "corrupt" after one retry,
    not a crash. Raises only on a hard local failure (download I/O, convert)."""
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
        return FetchResult("ok", conus=conus_gz)

    if not _release_published(date_obj):
        print(term.warn(
            f"source release for {date_obj:%Y-%m-%d} not published"))
        return FetchResult("missing", detail="release not published")

    global_gz = conus_dir / f"global_{date_compact}.gz"
    if not global_gz.exists():
        # Download → verify → extract, with ONE fresh-download retry. Two ways the
        # tar can be bad, and neither can be trusted to the extract's exit code
        # (tar exits 0 on truncation-as-EOF): a short download (size mismatch) and
        # a full-length-but-internally-corrupt archive (server-side rot — the size
        # check passes but tar's extract errors). We treat both the same: wipe the
        # parts, re-download once; if it still fails, skip the day as "corrupt"
        # (reported by the caller) instead of crashing or writing partial data.
        for attempt in (1, 2):
            print(term.stage(f"fetch {date_obj:%Y-%m-%d}: downloading tar parts"
                             + ("" if attempt == 1 else " (retry)")))
            if not download_tar_parts(date_obj, data_dir=str(conus_dir)):
                raise RuntimeError(
                    f"tar download failed for {date_obj:%Y-%m-%d} "
                    f"(release exists — likely a network/mount error)")

            size_ok = _tar_parts_ok(date_obj, conus_dir)
            if size_ok:
                print(term.stage(f"fetch {date_obj:%Y-%m-%d}: extracting traces"))
                if extract_traces(date_obj):
                    break                      # good tar — extracted cleanly
                reason = "extract failed (archive corrupt)"
            else:
                reason = "tar parts short/incomplete"

            # Bad tar this attempt. Wipe parts so the retry (or a later run) re-fetches.
            _remove_tar_parts(date_obj, conus_dir)
            if attempt == 2:
                print(term.warn(f"tar for {date_obj:%Y-%m-%d} still bad after "
                                f"re-download ({reason}) — skipping this day"))
                return FetchResult("corrupt", detail=reason)
            print(term.warn(f"tar for {date_obj:%Y-%m-%d} bad ({reason}) "
                            f"— re-downloading from scratch"))

        convert_traces_global(date_obj)

    print(term.stage(f"fetch {date_obj:%Y-%m-%d}: filtering to CONUS"))
    conus = convert_global_to_conus(date_obj)
    return FetchResult("ok", conus=conus)
