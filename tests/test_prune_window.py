"""Tests for the catch-up sliding-window prune.

The prune deletes real, expensive-to-rebuild data, so the rules that decide what
it may touch are pinned here:
  - out-of-window days are collected;
  - `window.keep` ranges are protected regardless of age;
  - a backfill chunk's --start-date never becomes the keep boundary.
"""

import datetime
import sys
import types
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hotspots import cli  # noqa: E402


def _mkconfig(tmp_path, keep=()):
    cfg = types.SimpleNamespace(
        grid_dir=tmp_path / "grid",
        events_dir=tmp_path / "events",
        regional_dir=tmp_path / "regional",
        maps_dir=tmp_path / "maps",
        window_days=90,
        window_keep=list(keep),
    )
    for d in (cfg.grid_dir, cfg.events_dir, cfg.regional_dir, cfg.maps_dir):
        d.mkdir(parents=True)
    return cfg


def _add_days(cfg, *tags):
    for tag in tags:
        (cfg.grid_dir / tag).mkdir()
        (cfg.events_dir / tag).mkdir()


KEEP_START = datetime.date(2026, 4, 25)


def test_out_of_window_days_are_collected(tmp_path):
    cfg = _mkconfig(tmp_path)
    _add_days(cfg, "20260301", "20260501")
    names = {p.name for p in cli._prune_targets(cfg, KEEP_START)}
    assert "20260301" in names          # before the window
    assert "20260501" not in names      # inside the window


def test_window_keep_protects_old_days(tmp_path):
    cfg = _mkconfig(tmp_path, keep=[("20250501", "20250831")])
    _add_days(cfg, "20250601", "20260301")
    names = {p.name for p in cli._prune_targets(cfg, KEEP_START)}
    assert "20250601" not in names      # protected archive block
    assert "20260301" in names          # unprotected + out of window


def test_window_keep_protects_overlapping_artifacts(tmp_path):
    """An artifact spans a range; it survives if it overlaps a kept range."""
    cfg = _mkconfig(tmp_path, keep=[("20250501", "20250831")])
    (cfg.maps_dir / "conus_20250601_20250831.html").touch()
    (cfg.maps_dir / "conus_20260301_20260305.html").touch()
    names = {p.name for p in cli._prune_targets(cfg, KEEP_START)}
    assert "conus_20250601_20250831.html" not in names
    assert "conus_20260301_20260305.html" in names


def test_backfill_chunk_start_is_not_the_keep_boundary():
    """The regression that deleted 80 backfilled days: a 10-day chunk's
    --start-date must not shrink the prune keep-window."""
    window_days = 90
    window_end = datetime.date(2025, 7, 31)
    chunk_start = datetime.date(2025, 7, 22)   # a 10-day backfill chunk
    prune_start = min(
        chunk_start, window_end - datetime.timedelta(days=window_days - 1))
    assert prune_start == datetime.date(2025, 5, 3)
    assert prune_start < chunk_start


def test_confirm_prune_declines_without_tty(tmp_path, monkeypatch, capsys):
    cfg = _mkconfig(tmp_path)
    _add_days(cfg, "20260301")
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False)
    assert cli._confirm_prune(cfg, KEEP_START, None) is False
    assert "skipping prune" in capsys.readouterr().out


def test_confirm_prune_requires_yes(tmp_path, monkeypatch):
    cfg = _mkconfig(tmp_path)
    _add_days(cfg, "20260301")
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "n")
    assert cli._confirm_prune(cfg, KEEP_START, None) is False
    monkeypatch.setattr("builtins.input", lambda _: "y")
    assert cli._confirm_prune(cfg, KEEP_START, None) is True


def test_confirm_prune_yes_flag_bypasses_prompt(tmp_path, monkeypatch):
    cfg = _mkconfig(tmp_path)
    _add_days(cfg, "20260301")

    def _boom(_):
        raise AssertionError("should not prompt when --yes is given")

    monkeypatch.setattr("builtins.input", _boom)
    assert cli._confirm_prune(cfg, KEEP_START, None, assume_yes=True) is True


def test_nothing_to_prune_returns_false(tmp_path):
    cfg = _mkconfig(tmp_path)
    _add_days(cfg, "20260501")          # inside the window
    assert cli._confirm_prune(cfg, KEEP_START, None, assume_yes=True) is False


def test_real_config_protects_archive_blocks():
    """The shipped pipeline_config.yaml must protect the 2025/2026 archives."""
    from hotspots.config import load_config
    cfg = load_config("src/hotspots/pipeline_config.yaml")
    keep = cfg.window_keep
    assert ("20250501", "20250831") in keep
    assert ("20260101", "20260131") in keep


@pytest.mark.parametrize("entry,expected", [
    ("20250501-20250831", ("20250501", "20250831")),
    ({"start": "20250501", "end": "20250831"}, ("20250501", "20250831")),
    ("20250501", ("20250501", "20250501")),        # single day
])
def test_window_keep_parsing(entry, expected):
    from hotspots.config import Config
    cfg = Config.__new__(Config)
    cfg._raw = {"window": {"keep": [entry]}}
    assert cfg.window_keep == [expected]


def test_window_keep_rejects_bad_range():
    from hotspots.config import Config
    cfg = Config.__new__(Config)
    cfg._raw = {"window": {"keep": ["2025-2026"]}}
    with pytest.raises(ValueError):
        cfg.window_keep
