"""End-to-end v2 pipeline tests on REAL ADS-B data.

These run the actual stages (LOS detection → aggregate → render) against the
committed WVI grid shards, in an isolated temp data root. They're slower than a
unit test (a few seconds each — real resampling) but exercise the whole chain
the way a real run does. See conftest.py for the fixture.
"""

import pandas as pd
import pytest

from conftest import run_cli
from hotspots.config import load_config


def _events_parquet(env, day):
    return env.events_dir / day / f"{day}_{env.cell}.parquet"


@pytest.mark.slow
def test_analyze_profile_builds_full_chain(pipeline_env):
    """`run analyze` (stages 3→4→5) on real data produces per-cell events, a
    regional parquet, and a map — all under the isolated data root."""
    env = pipeline_env

    run_cli(env.config_path, "run", "analyze", "--region", "wvi")

    # Stage 3: each fixture day has a per-cell events parquet with the real schema.
    for day in env.days:
        pq = _events_parquet(env, day)
        assert pq.exists(), f"missing events parquet for {day}"
        df = pd.read_parquet(pq)
        assert len(df) > 0, f"{day} produced no LOS events"
        for col in ("datetime_utc", "lat", "lon", "alt_band", "quality"):
            assert col in df.columns

    # Stage 4: a single regional parquet aggregating both days.
    regional = env.regional_dir / "wvi_20250601_20250602.parquet"
    assert regional.exists()
    region_df = pd.read_parquet(regional)
    per_day_total = sum(len(pd.read_parquet(_events_parquet(env, d)))
                        for d in env.days)
    assert len(region_df) == per_day_total

    # Stage 5: a non-trivial self-contained HTML map.
    html = env.maps_dir / "wvi_20250601_20250602.html"
    assert html.exists()
    assert html.stat().st_size > 50_000  # real map, not an empty stub

    # Nothing leaked outside the isolated root.
    assert str(env.data_root) in str(regional)


@pytest.mark.slow
def test_events_land_in_expected_geographic_cell(pipeline_env):
    """Sanity on the real data: detected events fall inside the WVI cell box."""
    env = pipeline_env
    run_cli(env.config_path, "run", "--from", "3", "--to", "3", "--region", "wvi")

    df = pd.read_parquet(_events_parquet(env, env.days[0]))
    # WVI cell is lat [36,37), lon [-122,-121). Allow a hair of slop for events
    # right at the boundary of the 1° shard.
    assert df["lat"].between(35.9, 37.1).all()
    assert df["lon"].between(-122.1, -120.9).all()


@pytest.mark.slow
def test_visualize_only_reuses_existing_regional(pipeline_env):
    """`visualize` (stage 5 only) must NOT recompute events — it just re-renders
    from the existing regional parquet. We assert by mtime."""
    env = pipeline_env

    # First build everything.
    run_cli(env.config_path, "run", "analyze", "--region", "wvi")
    events_mtime = _events_parquet(env, env.days[0]).stat().st_mtime
    regional = env.regional_dir / "wvi_20250601_20250602.parquet"
    regional_mtime = regional.stat().st_mtime

    # Then visualize-only.
    run_cli(env.config_path, "run", "visualize", "--region", "wvi")

    # Events + regional are untouched; only the map is (re)written.
    assert _events_parquet(env, env.days[0]).stat().st_mtime == events_mtime
    assert regional.stat().st_mtime == regional_mtime
    assert (env.maps_dir / "wvi_20250601_20250602.html").exists()


def test_config_loads_from_test_yaml(pipeline_env):
    """Cheap smoke test (no heavy run): the generated test config is valid and
    its data_root really is the isolated temp dir."""
    cfg = load_config(pipeline_env.config_path)
    assert cfg.data_root == pipeline_env.data_root
    assert cfg.region_bounds("wvi") == (36, 37, -122, -121)
    assert cfg.profile("analyze")["stages"] == [3, 4, 5]
