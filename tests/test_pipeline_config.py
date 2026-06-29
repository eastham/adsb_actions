"""Tests for the v2 pipeline config loader and the data-root redirection that
makes the test sandbox (and any --config) actually isolate stage I/O.

All fast — no real pipeline runs.
"""

import textwrap

import pytest

from hotspots import config as config_mod
from hotspots.config import load_config


def _write(tmp_path, body: str):
    p = tmp_path / "cfg.yaml"
    p.write_text(textwrap.dedent(body))
    return p


MINIMAL = """\
    paths: {data_root: data/v2, conus_dir: data}
    regions:
      wvi: {lat_min: 36, lat_max: 37, lon_min: -122, lon_max: -121}
    profiles:
      visualize: {stages: [5]}
    """


# --- loading & accessors ---------------------------------------------------

def test_loads_region_and_profile(tmp_path):
    cfg = load_config(_write(tmp_path, MINIMAL))
    assert cfg.region_bounds("wvi") == (36, 37, -122, -121)
    assert cfg.profile("visualize")["stages"] == [5]


def test_default_config_is_valid():
    """The real shipped pipeline_config.yaml must load and expose the documented
    regions/profiles (this is what the CLI uses out of the box)."""
    cfg = load_config()  # defaults to src/hotspots/pipeline_config.yaml
    assert "conus" in cfg.regions
    assert cfg.profile("analyze")["stages"] == [3, 4, 5]
    assert cfg.region_bounds("conus") == (24, 50, -125, -65)


def test_regions_dict_shape_matches_stage4(tmp_path):
    """stage4_aggregate consumes regions_dict() as {name: {lat_min,...}}."""
    cfg = load_config(_write(tmp_path, MINIMAL))
    d = cfg.regions_dict()
    assert d["wvi"]["lat_min"] == 36 and d["wvi"]["lon_max"] == -121


def test_path_accessors_are_absolute(tmp_path):
    cfg = load_config(_write(tmp_path, MINIMAL))
    # data_root is relative in the yaml but resolves against the project root.
    assert cfg.grid_dir.is_absolute()
    assert cfg.grid_dir.name == "grid"
    assert cfg.events_dir.parent == cfg.data_root


def test_unknown_region_and_profile_raise(tmp_path):
    cfg = load_config(_write(tmp_path, MINIMAL))
    with pytest.raises(KeyError):
        cfg.region_bounds("atlantis")
    with pytest.raises(KeyError):
        cfg.profile("teleport")


# --- validation ------------------------------------------------------------

def test_missing_section_rejected(tmp_path):
    with pytest.raises(ValueError):
        load_config(_write(tmp_path, "paths: {data_root: data/v2}\n"))  # no regions/profiles


def test_inverted_region_bounds_rejected(tmp_path):
    bad = """\
        paths: {data_root: data/v2, conus_dir: data}
        regions:
          oops: {lat_min: 40, lat_max: 30, lon_min: -120, lon_max: -110}
        profiles: {visualize: {stages: [5]}}
        """
    with pytest.raises(ValueError, match="inverted|empty"):
        load_config(_write(tmp_path, bad))


def test_invalid_profile_stage_rejected(tmp_path):
    bad = """\
        paths: {data_root: data/v2, conus_dir: data}
        regions: {wvi: {lat_min: 36, lat_max: 37, lon_min: -122, lon_max: -121}}
        profiles: {weird: {stages: [1, 9]}}
        """
    with pytest.raises(ValueError, match="invalid stages"):
        load_config(_write(tmp_path, bad))


def test_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "does_not_exist.yaml")


# --- data-root redirection (the isolation mechanism) -----------------------

def test_set_data_root_updates_constants_and_env(tmp_path, monkeypatch):
    target = tmp_path / "sandbox"
    config_mod.set_data_root(target)
    assert config_mod.EVENTS_DIR == target / "events"
    assert config_mod.GRID_DIR == target / "grid"
    import os
    assert os.environ["ADSB_V2_DATA_ROOT"] == str(target)


def test_set_data_root_refreshes_already_imported_stage_module(tmp_path):
    """The subtle one: a stage module that already did `from config import
    EVENTS_DIR` must still follow a later set_data_root — otherwise a sandbox
    redirect would silently leak writes to the real data/v2."""
    config_mod.set_data_root(tmp_path / "rootA")
    import hotspots.stage3_analyze as s3
    assert s3.EVENTS_DIR == tmp_path / "rootA" / "events"

    config_mod.set_data_root(tmp_path / "rootB")
    assert s3.EVENTS_DIR == tmp_path / "rootB" / "events"
