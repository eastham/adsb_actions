"""Tests for the day-at-a-time verification gate — the safety net that catches
incomplete stage output (e.g. when the network drive drops mid-run) before it
flows into aggregation.

verify_day() is tested directly with hand-built files (fast). The retry/abort
behavior is tested through the real CLI but without a heavy LOS run, by asking
it to process a day whose grid shard doesn't exist.
"""

import gzip

import pandas as pd
import pytest

from conftest import run_cli
from hotspots.verify import verify_day


def _bounds():
    # WVI single cell: lat [36,37), lon [-122,-121).
    return (36, 37, -122, -121)


def _write_gz(path, content=b"hello"):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as f:
        f.write(content)


def _write_parquet(path, rows=1):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"x": range(rows)}).to_parquet(path)


# --- stage 2 (shards) ------------------------------------------------------

def test_stage2_present_when_shard_exists(tmp_path):
    grid = tmp_path / "grid"
    _write_gz(grid / "20250601" / "20250601_36_-122.gz")
    rpt = verify_day(2, "20250601", _bounds(), grid, tmp_path / "events")
    assert rpt.ok
    assert rpt.present_ok == 1 and not rpt.missing


def test_stage2_missing_when_shard_absent(tmp_path):
    rpt = verify_day(2, "20250601", _bounds(), tmp_path / "grid",
                     tmp_path / "events")
    assert not rpt.ok
    assert rpt.missing == ["36_-122"]


def test_stage2_truncated_gz_flagged_under_sanity(tmp_path):
    """A zero-byte (truncated-on-disconnect) shard fails the deep sanity check
    but would pass an existence-only check."""
    grid = tmp_path / "grid"
    p = grid / "20250601" / "20250601_36_-122.gz"
    p.parent.mkdir(parents=True)
    p.write_bytes(b"")  # exists but empty / not valid gzip

    assert not verify_day(2, "20250601", _bounds(), grid, tmp_path / "events",
                          sanity=True).ok
    assert verify_day(2, "20250601", _bounds(), grid, tmp_path / "events",
                      sanity=False).ok


# --- stage 3 (events): parquet OR .empty sentinel both count ----------------

def test_stage3_parquet_counts_as_present(tmp_path):
    events = tmp_path / "events"
    _write_parquet(events / "20250601" / "20250601_36_-122.parquet")
    rpt = verify_day(3, "20250601", _bounds(), tmp_path / "grid", events)
    assert rpt.ok
    assert rpt.present_ok == 1 and rpt.present_empty == 0


def test_stage3_empty_sentinel_counts_as_present(tmp_path):
    """A cell with no LOS events writes a `.empty` sentinel — that's a completed
    cell, NOT a missing one. This is what keeps 'no events' from looking like a
    drive drop."""
    events = tmp_path / "events"
    sentinel = events / "20250601" / "20250601_36_-122.empty"
    sentinel.parent.mkdir(parents=True)
    sentinel.touch()
    rpt = verify_day(3, "20250601", _bounds(), tmp_path / "grid", events)
    assert rpt.ok
    assert rpt.present_empty == 1 and not rpt.missing


def test_stage3_missing_when_neither_exists(tmp_path):
    rpt = verify_day(3, "20250601", _bounds(), tmp_path / "grid",
                     tmp_path / "events")
    assert not rpt.ok
    assert rpt.missing == ["36_-122"]


def test_stage3_corrupt_parquet_flagged_under_sanity(tmp_path):
    events = tmp_path / "events"
    p = events / "20250601" / "20250601_36_-122.parquet"
    p.parent.mkdir(parents=True)
    p.write_bytes(b"not a parquet file")
    assert not verify_day(3, "20250601", _bounds(), tmp_path / "grid", events,
                          sanity=True).ok


def test_stage3_excluded_cell_not_counted_missing(tmp_path):
    """A CELL_EXCLUSIONS cell produces no stage-3 output by design; the gate
    must treat it as excluded (not expected, not missing) so the day still
    passes. Previously this cell was flagged missing and burned all retries."""
    from hotspots.exclusions import CELL_EXCLUSIONS
    ex_lat, ex_lon, start, _end, _reason = CELL_EXCLUSIONS[0]
    bounds = (ex_lat, ex_lat + 1, ex_lon, ex_lon + 1)  # single excluded cell
    rpt = verify_day(3, start, bounds, tmp_path / "grid", tmp_path / "events")
    assert rpt.ok
    assert rpt.expected == 0 and rpt.excluded == 1 and not rpt.missing


def test_stage2_still_verifies_excluded_cell(tmp_path):
    """Stage 2 shards excluded cells (only stage 3 skips them), so an absent
    shard there is still a real problem the gate should catch."""
    from hotspots.exclusions import CELL_EXCLUSIONS
    ex_lat, ex_lon, start, _end, _reason = CELL_EXCLUSIONS[0]
    bounds = (ex_lat, ex_lat + 1, ex_lon, ex_lon + 1)
    rpt = verify_day(2, start, bounds, tmp_path / "grid", tmp_path / "events")
    assert not rpt.ok
    assert rpt.expected == 1 and rpt.missing == [f"{ex_lat}_{ex_lon}"]


def test_summary_string_is_informative(tmp_path):
    events = tmp_path / "events"
    _write_parquet(events / "20250601" / "20250601_36_-122.parquet")
    rpt = verify_day(3, "20250601", _bounds(), tmp_path / "grid", events)
    s = rpt.summary()
    assert "events:" in s and "1/1" in s


# --- the gate's retry + abort, through the real CLI ------------------------

def test_gate_aborts_on_missing_day(pipeline_env, capsys):
    """Ask the CLI to analyze a day with no grid shard: stage 3 produces nothing,
    the gate retries (retry_attempts=2 in the test config) and then aborts with a
    nonzero exit and a precise re-run command."""
    env = pipeline_env
    missing_day = "20250609"  # not in the fixture

    with pytest.raises(SystemExit) as excinfo:
        run_cli(env.config_path, "run", "--from", "3", "--to", "3",
                "--region", "wvi",
                "--start-date", missing_day, "--end-date", missing_day)

    # The abort detail rides on the SystemExit message (pytest intercepts the
    # exit before the interpreter would print it to stderr).
    msg = str(excinfo.value)
    assert "ABORT" in msg
    assert missing_day in msg
    # The abort message hands back a copy-pasteable single-day re-run.
    assert "run --from 3 --region wvi" in msg

    # The progress output shows it really retried, not gave up on attempt 1.
    combined = capsys.readouterr().out
    assert "attempt 2/2" in combined
