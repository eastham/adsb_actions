"""Tests for data quality assessment.

Uses pre-fetched KWVI fixtures (3 weekend dates) so no network access is
needed.  KWVI: lat=36.9369, lon=-121.7897, field_elev=159ft.
"""

import shutil
from pathlib import Path

import pytest

from src.tools.data_quality import (
    analyze_shard_quality,
    build_data_quality,
    _score_termination,
    _score_gap,
    _score_confidence,
    _overall_score,
)

FIXTURES = Path("tests/fixtures/KWVI")
KWVI_LAT = 36.9369
KWVI_LON = -121.7897
KWVI_ELEV = 159


# --- Unit tests for scoring helpers ---

def test_score_termination():
    assert _score_termination(0.10) == "green"
    assert _score_termination(0.24) == "green"
    assert _score_termination(0.25) == "yellow"
    assert _score_termination(0.30) == "yellow"
    assert _score_termination(0.49) == "yellow"
    assert _score_termination(0.50) == "red"
    assert _score_termination(0.80) == "red"
    assert _score_termination(None) == "yellow"


def test_score_gap():
    assert _score_gap(2.0) == "green"
    assert _score_gap(4.9) == "green"
    assert _score_gap(5.0) == "yellow"
    assert _score_gap(10.0) == "yellow"
    assert _score_gap(14.9) == "yellow"
    assert _score_gap(15.0) == "red"
    assert _score_gap(30.0) == "red"
    assert _score_gap(None) == "yellow"


def test_score_confidence():
    assert _score_confidence(10) == "green"
    assert _score_confidence(5) == "green"
    assert _score_confidence(4) == "yellow"
    assert _score_confidence(3) == "yellow"
    assert _score_confidence(2) == "red"
    assert _score_confidence(1) == "red"


def test_overall_score():
    assert _overall_score("green", "green", "green") == "green"
    assert _overall_score("green", "yellow", "green") == "yellow"
    assert _overall_score("green", "green", "red") == "red"
    assert _overall_score("red", "red", "red") == "red"
    assert _overall_score("yellow", "red", "green") == "red"


# --- Shard analysis tests ---

def test_analyze_shard_quality():
    shard = FIXTURES / "060125_KWVI.gz"
    assert shard.exists(), f"Missing fixture: {shard}"

    result = analyze_shard_quality(shard, field_elev=KWVI_ELEV,
                                   airport_lat=KWVI_LAT,
                                   airport_lon=KWVI_LON)
    assert result is not None
    assert result["total_tracks"] > 0
    assert result["total_gaps"] > 0
    assert result["median_gap_s"] is not None
    assert result["median_gap_s"] > 0
    assert result["p90_gap_s"] is not None
    assert result["p90_gap_s"] >= result["median_gap_s"]


def test_analyze_shard_quality_low_alt_tracks():
    """Verify we detect some low-altitude track candidates in the KWVI data."""
    shard = FIXTURES / "060125_KWVI.gz"
    result = analyze_shard_quality(shard, field_elev=KWVI_ELEV,
                                   airport_lat=KWVI_LAT,
                                   airport_lon=KWVI_LON)
    assert result is not None
    # KWVI should have at least some low-altitude tracks
    assert result["low_alt_tracks"] > 0
    if result["lost_rate"] is not None:
        assert 0.0 <= result["lost_rate"] <= 1.0


def test_analyze_shard_quality_nonexistent():
    result = analyze_shard_quality(Path("/nonexistent/file.gz"),
                                   field_elev=0,
                                   airport_lat=0.0, airport_lon=0.0)
    assert result is None


# --- Integration test ---

def test_build_data_quality(tmp_path):
    """Full integration: build data quality from fixtures."""
    airport_dir = tmp_path / "KWVI"
    airport_dir.mkdir()
    for f in FIXTURES.iterdir():
        shutil.copy2(f, airport_dir / f.name)

    result = build_data_quality("KWVI", airport_dir,
                                field_elev=KWVI_ELEV,
                                airport_lat=KWVI_LAT,
                                airport_lon=KWVI_LON)

    assert result is not None
    assert result["icao"] == "KWVI"
    assert result["numDates"] == 3
    assert result["score"] in ("green", "yellow", "red")
    assert result["totalLowAltTracks"] > 0

    # Check sub-scores exist
    details = result["details"]
    assert details["terminationScore"] in ("green", "yellow", "red")
    assert details["gapScore"] in ("green", "yellow", "red")
    assert details["confidenceScore"] in ("green", "yellow", "red")

    # Gap metrics should be present
    assert result["medianGapS"] is not None
    assert result["medianGapS"] > 0


def test_build_data_quality_empty(tmp_path):
    """Empty directory returns None."""
    airport_dir = tmp_path / "EMPTY"
    airport_dir.mkdir()
    result = build_data_quality("EMPTY", airport_dir, field_elev=0,
                                airport_lat=0.0, airport_lon=0.0)
    assert result is None


def test_quality_in_html(tmp_path):
    """Verify visualizer generates with busyness and quality data."""
    import json
    import subprocess

    from src.tools.busyness import build_busyness_data

    airport_dir = tmp_path / "KWVI"
    airport_dir.mkdir()
    for f in FIXTURES.iterdir():
        shutil.copy2(f, airport_dir / f.name)

    busyness_data = build_busyness_data("KWVI", airport_dir,
                                        metar_year=2025,
                                        metar_cache_dir=airport_dir,
                                        field_elev=KWVI_ELEV)
    busyness_json = airport_dir / "KWVI_busyness.json"
    busyness_json.write_text(json.dumps(busyness_data))

    quality_data = build_data_quality("KWVI", airport_dir,
                                      field_elev=KWVI_ELEV,
                                      airport_lat=KWVI_LAT,
                                      airport_lon=KWVI_LON)
    quality_json = airport_dir / "KWVI_quality.json"
    quality_json.write_text(json.dumps(quality_data))

    # Create a minimal traffic sample so the visualizer doesn't exit early
    traffic_file = airport_dir / "traffic.csv"
    traffic_file.write_text(
        '[[36.93,-121.79,1000],[36.94,-121.78,800]]\n'
    )

    output_html = airport_dir / "test_map.html"
    result = subprocess.run(
        [
            "python", "src/postprocessing/visualizer.py",
            "--sw", "36.85,-121.87",
            "--ne", "36.97,-121.72",
            "--busyness-data", str(busyness_json),
            "--data-quality", str(quality_json),
            "--traffic-samples", str(traffic_file),
            "--output", str(output_html),
            "--no-browser",
        ],
        input="",
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"Visualizer failed: {result.stderr}"
    assert output_html.exists()

    html = output_html.read_text()
    # Verify busyness chart is present
    assert "busyness-chart" in html
    assert "Typical Traffic" in html
