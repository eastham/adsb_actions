"""Tests for v2 pipeline provenance — the algorithm-version tracking that lets
`status` warn when a regional map mixes detector versions.

Mostly fast unit tests against provenance.py / status.py, plus one slow real-run
test that nails down the trickiest behavior: --skip-existing must NOT re-stamp a
cell it didn't actually recompute.
"""

import json

import pytest

from conftest import run_cli
from hotspots import provenance as prov
from hotspots import status as status_mod
from hotspots.config import load_config


# --- provenance.py unit tests (fast, no real data) -------------------------

def test_merge_preserves_other_cells(tmp_path):
    """The core correctness property: writing one cell's provenance must not
    clobber another cell's entry already in the same date manifest.
    (This is the WVI-re-run-into-a-CONUS-day case.)"""
    day_dir = tmp_path / "20250601"

    prov.merge_cell_provenance(day_dir, {
        "37_-122": {"git_sha": "aaa", "git_dirty": False,
                    "config_hash": "c1", "written_utc": "t1"},
    })
    prov.merge_cell_provenance(day_dir, {
        "36_-122": {"git_sha": "bbb", "git_dirty": False,
                    "config_hash": "c2", "written_utc": "t2"},
    })

    manifest = prov.read_provenance(day_dir)
    assert set(manifest) == {"37_-122", "36_-122"}
    assert manifest["37_-122"]["git_sha"] == "aaa"   # survived untouched
    assert manifest["36_-122"]["git_sha"] == "bbb"


def test_read_provenance_missing_is_none(tmp_path):
    """Pre-tagging dirs (no manifest) read as None, not an error — backward compat."""
    assert prov.read_provenance(tmp_path / "nope") is None


def test_current_provenance_has_expected_fields(pipeline_env):
    cfg = load_config(pipeline_env.config_path)
    rec = prov.current_provenance(cfg)
    assert set(rec) == {"git_sha", "git_dirty", "config_hash", "written_utc"}
    # In this repo git is available, so a SHA and a real dirty flag are present.
    assert rec["git_sha"] is not None
    assert isinstance(rec["git_dirty"], bool)


def test_config_hash_changes_with_alt_ceiling(pipeline_env):
    """The config hash should distinguish runs made with different params, so a
    'same SHA but different settings' situation is still detectable."""
    cfg = load_config(pipeline_env.config_path)
    h1 = prov.config_hash(cfg)
    # Mutate the in-memory config and re-hash.
    cfg._raw["runtime"]["alt_ceiling_ft"] = 12000
    h2 = prov.config_hash(cfg)
    assert h1 != h2


# --- status mixing detection (fast; hand-built manifests) ------------------

def _seed_manifest(events_dir, day, cell_to_sha, dirty=False):
    day_dir = events_dir / day
    records = {cell: {"git_sha": sha, "git_dirty": dirty,
                      "config_hash": "c", "written_utc": "t"}
               for cell, sha in cell_to_sha.items()}
    prov.merge_cell_provenance(day_dir, records)


def test_status_detects_mixed_versions(pipeline_env):
    """Two days at different SHAs → status emits the MIXED warning naming both
    SHAs and a rebuild command scoped to the stale day."""
    env = pipeline_env
    cfg = load_config(env.config_path)
    _seed_manifest(env.events_dir, "20250601", {env.cell: "OLD0001"})
    _seed_manifest(env.events_dir, "20250602", {env.cell: prov.git_sha()})

    lines = status_mod.report(cfg, "wvi", cfg.region_bounds("wvi"),
                              "20250601", "20250602")
    text = "\n".join(lines)
    assert "Provenance MIXED" in text
    assert "OLD0001" in text
    # The fix command targets only the stale day.
    assert "run analyze" in text
    assert "--start-date 20250601" in text


def test_status_clean_when_single_version(pipeline_env):
    env = pipeline_env
    cfg = load_config(env.config_path)
    sha = prov.git_sha()
    _seed_manifest(env.events_dir, "20250601", {env.cell: sha})
    _seed_manifest(env.events_dir, "20250602", {env.cell: sha})

    text = "\n".join(status_mod.report(cfg, "wvi", cfg.region_bounds("wvi"),
                                       "20250601", "20250602"))
    assert "Provenance MIXED" not in text
    assert "all events at" in text
    assert "dirty" not in text  # all clean → no dirty note


def test_status_warns_when_cells_built_dirty(pipeline_env):
    """A clean SHA match must still flag cells built from a dirty tree — the SHA
    alone can't capture uncommitted code. (Same-SHA, so NOT a 'mixed' warning.)"""
    env = pipeline_env
    cfg = load_config(env.config_path)
    sha = prov.git_sha()
    _seed_manifest(env.events_dir, "20250601", {env.cell: sha}, dirty=True)
    _seed_manifest(env.events_dir, "20250602", {env.cell: sha}, dirty=False)

    text = "\n".join(status_mod.report(cfg, "wvi", cfg.region_bounds("wvi"),
                                       "20250601", "20250602"))
    assert "Provenance MIXED" not in text       # same SHA → not mixed
    assert "all events at" in text              # clean match still shown
    assert "dirty" in text                       # but dirtiness surfaced
    assert "1 cell" in text                      # exactly the one dirty cell


def test_status_out_of_box_cells_ignored(pipeline_env):
    """A stale SHA on a cell OUTSIDE the queried region must not trigger a
    mixing warning for that region."""
    env = pipeline_env
    cfg = load_config(env.config_path)
    sha = prov.git_sha()
    # In-box cell current; an out-of-box neighbor at an old SHA.
    _seed_manifest(env.events_dir, "20250601",
                   {env.cell: sha, "40_-100": "OLD0002"})

    text = "\n".join(status_mod.report(cfg, "wvi", cfg.region_bounds("wvi"),
                                       "20250601", "20250601"))
    assert "Provenance MIXED" not in text
    assert "OLD0002" not in text


# --- traffic-tile build provenance -----------------------------------------

def _tiles_config(tmp_path, tiles_dir):
    """A config whose data_root makes <project_root> resolve to tmp_path, and
    whose traffic_tiles_local points (absolutely) at tiles_dir — so status's
    relative-path resolution is exercised end to end."""
    import textwrap
    from hotspots.config import load_config
    cfg_path = tmp_path / "cfg.yaml"
    cfg_path.write_text(textwrap.dedent(f"""\
        paths:
          data_root: {tmp_path}/data/v2
          conus_dir: {tmp_path}/data
          traffic_tiles_local: {tiles_dir}
        regions: {{wvi: {{lat_min: 36, lat_max: 37, lon_min: -122, lon_max: -121}}}}
        profiles: {{visualize: {{stages: [5]}}}}
        """))
    return load_config(cfg_path)


def test_status_reports_tile_build_and_flags_dirty(tmp_path):
    tiles = tmp_path / "tiles" / "traffic"
    prov.write_provenance(tiles, {
        "tool": "traffic_tiles", "git_sha": "abc1234", "git_dirty": True,
        "built_utc": "2026-06-28T12:00:00", "tuning": {"color_vibrancy": 0.9},
    })
    cfg = _tiles_config(tmp_path, tiles)

    text = "\n".join(status_mod.report(cfg, "wvi", cfg.region_bounds("wvi"),
                                       "20250601", "20250601"))
    assert "Traffic tiles: built at abc1234" in text
    assert "DIRTY" in text


def test_status_tile_clean_build_no_warning(tmp_path):
    tiles = tmp_path / "tiles" / "traffic"
    prov.write_provenance(tiles, {
        "tool": "traffic_tiles", "git_sha": "abc1234", "git_dirty": False,
        "built_utc": "2026-06-28T12:00:00",
    })
    cfg = _tiles_config(tmp_path, tiles)
    text = "\n".join(status_mod.report(cfg, "wvi", cfg.region_bounds("wvi"),
                                       "20250601", "20250601"))
    assert "Traffic tiles: built at abc1234" in text
    assert "DIRTY" not in text


def test_status_tile_untagged_when_no_provenance(tmp_path):
    tiles = tmp_path / "tiles" / "traffic"
    tiles.mkdir(parents=True)  # dir exists but no _provenance.json
    cfg = _tiles_config(tmp_path, tiles)
    text = "\n".join(status_mod.report(cfg, "wvi", cfg.region_bounds("wvi"),
                                       "20250601", "20250601"))
    assert "untagged build" in text


# --- the skip-existing no-restamp guarantee (slow; real run) ---------------

@pytest.mark.slow
def test_skip_existing_does_not_restamp_provenance(pipeline_env):
    """Regression test for a real bug: a cell skipped via --skip-existing must
    keep its prior SHA. Re-stamping it with the current SHA would falsely claim
    the old output was produced by the current code."""
    env = pipeline_env
    day = env.days[0]
    manifest_path = env.events_dir / day / "_provenance.json"

    # Build everything once (real LOS run), so the parquet + manifest exist.
    run_cli(env.config_path, "run", "analyze", "--region", "wvi")
    assert manifest_path.exists()

    # Forge an old SHA for the cell, as if it were built by an earlier version.
    data = json.loads(manifest_path.read_text())
    data[env.cell]["git_sha"] = "ANCIENT"
    manifest_path.write_text(json.dumps(data))

    # Re-run stage 3 WITH --skip-existing: the parquet already exists, so the
    # detector skips this cell and must not touch its provenance.
    run_cli(env.config_path, "run", "--from", "3", "--to", "3",
            "--region", "wvi", "--skip-existing")

    after = json.loads(manifest_path.read_text())
    assert after[env.cell]["git_sha"] == "ANCIENT", \
        "skipped cell was wrongly re-stamped with the current SHA"
