"""Known cell/date exclusions for the v2 pipeline.

Some cells on some dates host high-density events (e.g. a fly-in) that flood the
prox detector with false-positive LOS events. Stage 3 skips analyzing those
cells. This module is the single source of truth for that list so both the
analyzer (pipeline.py, which skips them) and the completeness verifier
(verify.py, which must NOT count a deliberately-skipped cell as missing) agree.

Kept dependency-free (no pandas / stage imports) so verify.py can import it
without pulling in the heavy pipeline module.
"""

from pathlib import Path

# Each entry: (lat, lon, start_date_YYYYMMDD, end_date_YYYYMMDD, reason)
CELL_EXCLUSIONS = [
    (43, -89, "20250719", "20250727", "EAA AirVenture Oshkosh"),
    (43, -89, "20260718", "20260728", "EAA AirVenture Oshkosh"),
]


def is_excluded(lat: int, lon: int, date_tag: str) -> tuple[bool, str]:
    """Return (excluded, reason) for a cell on a date."""
    for ex_lat, ex_lon, start, end, reason in CELL_EXCLUSIONS:
        if lat == ex_lat and lon == ex_lon and start <= date_tag <= end:
            return True, reason
    return False, ""


def is_excluded_path(path: Path, date_tag: str) -> bool:
    """Exclusion check keyed off a shard/event path stem (<date>_<lat>_<lon>)."""
    parts = path.stem.split("_")
    if len(parts) < 3:
        return False
    try:
        lat, lon = int(parts[1]), int(parts[2])
    except ValueError:
        return False
    return is_excluded(lat, lon, date_tag)[0]
