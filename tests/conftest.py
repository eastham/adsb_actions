"""Shared helpers for the v2 pipeline tests (test_pipeline_*.py).

These tests exercise the real pipeline on real ADS-B data: a couple of WVI grid
shards committed under tests/fixtures/v2_pipeline/grid/. Each test gets its own
temp data root (seeded with copies of those shards) and a generated config
pointing at it, so runs are fully isolated and never touch the project's data/v2.
"""

import shutil
import textwrap
from pathlib import Path

import pytest

FIXTURE_GRID = Path(__file__).resolve().parent / "fixtures" / "v2_pipeline" / "grid"

# The single cell our fixture covers, and the two days available.
WVI_CELL = "36_-122"
FIXTURE_DAYS = ["20250601", "20250602"]


def write_test_config(data_root: Path, conus_dir: Path) -> Path:
    """Write a pipeline_config.yaml pointing at `data_root`, return its path.
    Retries are fast (0 pause) and remount is a no-op so the gate tests are quick.
    """
    cfg = textwrap.dedent(f"""\
        paths:
          data_root: {data_root}
          conus_dir: {conus_dir}
          traffic_tiles_url: https://example.invalid/tiles
        deploy:
          remote: test:bucket
          maps_dir: {data_root}/maps
        runtime:
          workers: 1
          alt_ceiling_ft: 18000
          remount_cmd: ""
          retry_attempts: 2
          retry_pause_s: 0
        dates:
          default_start: "{FIXTURE_DAYS[0]}"
          default_end:   "{FIXTURE_DAYS[-1]}"
        regions:
          wvi:   {{lat_min: 36, lat_max: 37, lon_min: -122, lon_max: -121}}
          conus: {{lat_min: 24, lat_max: 50, lon_min: -125, lon_max: -65}}
        profiles:
          visualize:     {{stages: [5], pmtiles: false}}
          aggregate-viz: {{stages: [4, 5], pmtiles: false}}
          analyze:       {{stages: [3, 4, 5], pmtiles: false}}
          full:          {{stages: [2, 3, 4, 5], pmtiles: false}}
        """)
    path = data_root / "pipeline_config_test.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(cfg)
    return path


@pytest.fixture
def pipeline_env(tmp_path):
    """An isolated data root seeded with the fixture grid shards + a config.

    Yields a small namespace: .data_root, .config_path, .days, and convenience
    paths under the root. Each test gets a fresh tmp_path, so nothing leaks.
    """
    data_root = tmp_path / "v2"
    grid = data_root / "grid"
    for day in FIXTURE_DAYS:
        dest = grid / day
        dest.mkdir(parents=True)
        shutil.copy2(FIXTURE_GRID / day / f"{day}_{WVI_CELL}.gz",
                     dest / f"{day}_{WVI_CELL}.gz")

    config_path = write_test_config(data_root, conus_dir=tmp_path / "no_conus")

    class Env:
        pass

    env = Env()
    env.data_root = data_root
    env.config_path = config_path
    env.days = list(FIXTURE_DAYS)
    env.cell = WVI_CELL
    env.grid_dir = data_root / "grid"
    env.events_dir = data_root / "events"
    env.regional_dir = data_root / "regional"
    env.maps_dir = data_root / "maps"
    return env


def run_cli(config_path, *args):
    """Invoke cli.main() with --config and the given args, in-process.

    cli.main() reloads config + set_data_root each call, so successive calls in a
    test stay pointed at the test's data root.
    """
    from hotspots import cli
    cli.main(["--config", str(config_path), *args])


@pytest.fixture(autouse=True)
def _restore_data_root():
    """set_data_root() mutates global state in hotspots.config (and $ADSB_V2_
    DATA_ROOT). Restore it after every test so a pipeline test can't leak its
    sandbox root into unrelated tests."""
    import os
    from hotspots import config as _config

    saved_root = _config.DATA_ROOT
    saved_env = os.environ.get("ADSB_V2_DATA_ROOT")
    try:
        yield
    finally:
        _config.set_data_root(saved_root)
        if saved_env is None:
            os.environ.pop("ADSB_V2_DATA_ROOT", None)
        else:
            os.environ["ADSB_V2_DATA_ROOT"] = saved_env
