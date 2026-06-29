"""Expected-vs-present verification for v2 pipeline stage outputs.

Shared by two callers:
  - the day-at-a-time orchestration gate in cli.py (after running a stage for a
    day, confirm every expected cell landed before moving on / aggregating), and
  - the status reporter (status.py), which reports the same counts read-only.

The check distinguishes three per-cell states so a network-drive disconnect
(missing/truncated output) is never confused with a cell that legitimately has
no events:
  - PRESENT_OK   : output exists and passes a cheap sanity test
  - PRESENT_EMPTY: stage-3 wrote the `.empty` sentinel (no events — fine)
  - MISSING       : output absent or failed sanity (likely a drive drop)
"""

import gzip
from dataclasses import dataclass, field
from pathlib import Path


def cells_in_box(lat_min, lat_max, lon_min, lon_max):
    """Yield (lat, lon) integer cell corners in the bounding box.
    Inclusive min / exclusive max, matching the sharding convention."""
    for lat in range(lat_min, lat_max):
        for lon in range(lon_min, lon_max):
            yield lat, lon


def _gz_ok(path: Path) -> bool:
    """Non-zero and has a readable gzip header (catches mid-write truncation)."""
    try:
        if path.stat().st_size == 0:
            return False
        with gzip.open(path, "rb") as f:
            f.read(1)
        return True
    except Exception:
        return False


def _parquet_ok(path: Path) -> bool:
    """Non-zero and has a readable parquet footer."""
    try:
        if path.stat().st_size == 0:
            return False
        import pyarrow.parquet as pq
        pq.read_metadata(str(path))
        return True
    except Exception:
        return False


@dataclass
class DayReport:
    stage: int
    date_tag: str
    expected: int = 0
    present_ok: int = 0
    present_empty: int = 0
    missing: list = field(default_factory=list)   # list of cell_tag strings

    @property
    def ok(self) -> bool:
        return not self.missing

    @property
    def accounted(self) -> int:
        return self.present_ok + self.present_empty

    def summary(self) -> str:
        if self.stage == 2:
            return f"shards: {self.present_ok}/{self.expected} cells present"
        return (f"events: {self.accounted}/{self.expected} cells "
                f"({self.present_ok} with events, {self.present_empty} empty, "
                f"{len(self.missing)} missing)")


def verify_day(stage: int, date_tag: str, bounds, grid_dir: Path,
               events_dir: Path, sanity: bool = True) -> DayReport:
    """Verify a single day's stage output across all cells in `bounds`.

    stage 2 expects <grid_dir>/<date>/<date>_<lat>_<lon>.gz per cell.
    stage 3 expects, per cell, EITHER <events_dir>/<date>/<date>_<lat>_<lon>.parquet
            OR the `.empty` sentinel (no events). Either counts as accounted.

    `sanity=True` (the orchestration gate) opens each present file to confirm it
    isn't truncated — correct but slow on a network mount at CONUS scale.
    `sanity=False` (status overviews) checks existence only, which is fast.
    """
    lat_min, lat_max, lon_min, lon_max = bounds
    rpt = DayReport(stage=stage, date_tag=date_tag)

    for lat, lon in cells_in_box(lat_min, lat_max, lon_min, lon_max):
        rpt.expected += 1
        cell_tag = f"{lat}_{lon}"
        stem = f"{date_tag}_{cell_tag}"

        if stage == 2:
            gz = grid_dir / date_tag / f"{stem}.gz"
            if gz.exists() and (not sanity or _gz_ok(gz)):
                rpt.present_ok += 1
            else:
                rpt.missing.append(cell_tag)

        elif stage == 3:
            day_dir = events_dir / date_tag
            parquet = day_dir / f"{stem}.parquet"
            empty = day_dir / f"{stem}.empty"
            if parquet.exists():
                if not sanity or _parquet_ok(parquet):
                    rpt.present_ok += 1
                else:
                    rpt.missing.append(cell_tag)   # truncated parquet
            elif empty.exists():
                rpt.present_empty += 1
            else:
                rpt.missing.append(cell_tag)

        else:
            raise ValueError(f"verify_day only handles stages 2 and 3, got {stage}")

    return rpt
