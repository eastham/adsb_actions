"""Tests for the airport-quality / runway-usage integration in the new CLI.

The heavy compute (build_v2_airport_quality) is never triggered by `run` — the
CLI only aggregates a cached per-day scores dir or loads an explicit JSON. These
tests cover that resolution logic + the config path wiring, without needing the
real (hours-long) compute or the real aq/ cache.
"""

import datetime
import json
from types import SimpleNamespace

import pytest

from hotspots import config as config_mod
from hotspots import cli
from hotspots.config import load_config


def _aq_args(path=None, workers=1):
    return SimpleNamespace(airport_quality=True, airport_quality_path=path,
                           workers=workers)


def test_aq_dir_follows_config_and_set_data_root(tmp_path):
    """The airport-quality dir is config-derived and honors set_data_root, so a
    sandbox redirect keeps it off the real data/v2/aq."""
    config_mod.set_data_root(tmp_path / "rootA")
    assert config_mod.AQ_DIR == tmp_path / "rootA" / "aq"

    # v2_airport_quality's own constant must track it too (it declares V2_AQ_DIR).
    import tools.v2_airport_quality as aq
    config_mod.set_data_root(tmp_path / "rootB")
    assert aq.V2_AQ_DIR == tmp_path / "rootB" / "aq"


def test_aq_skips_gracefully_with_empty_cache(tmp_path, capsys):
    """No cached per-day files → resolver returns None and prints the exact
    `--mode compute` command, never triggering a multi-hour build."""
    cfg = _isolated_config(tmp_path)
    (cfg.aq_dir).mkdir(parents=True)  # dir exists but empty

    result = cli._resolve_airport_quality(
        cfg, _aq_args(), datetime.date(2025, 6, 1), datetime.date(2025, 6, 2))

    assert result is None
    out = capsys.readouterr().out
    assert "skipping airport quality" in out
    assert "--mode compute" in out


def test_aq_explicit_path_loaded_verbatim(tmp_path):
    """--airport-quality-path bypasses aggregation and loads the JSON as-is."""
    cfg = _isolated_config(tmp_path)
    aq_json = tmp_path / "prebuilt.json"
    aq_json.write_text(json.dumps({"KWVI": {"quality": "high"}}))

    result = cli._resolve_airport_quality(
        cfg, _aq_args(path=str(aq_json)),
        datetime.date(2025, 6, 1), datetime.date(2025, 6, 2))

    assert result == {"KWVI": {"quality": "high"}}


def test_aq_explicit_path_missing_raises(tmp_path):
    cfg = _isolated_config(tmp_path)
    with pytest.raises(SystemExit):
        cli._resolve_airport_quality(
            cfg, _aq_args(path=str(tmp_path / "nope.json")),
            datetime.date(2025, 6, 1), datetime.date(2025, 6, 2))


def _isolated_config(tmp_path):
    """A config rooted under tmp_path so nothing touches real data/v2."""
    import textwrap
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(textwrap.dedent(f"""\
        paths: {{data_root: {tmp_path}/v2, conus_dir: {tmp_path}/none}}
        regions: {{wvi: {{lat_min: 36, lat_max: 37, lon_min: -122, lon_max: -121}}}}
        profiles: {{visualize: {{stages: [5]}}}}
        """))
    return load_config(cfg_path)
