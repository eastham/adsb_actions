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
    _summarize_runway_usage,
    _score_termination,
    _score_gap,
    _overall_score,
)
from src.tools.runway_usage import (
    build_runway_boxes,
    runway_votes_for_track,
    APPROACH_SPLIT_GAP_S,
    _approach_runway,
    _longest_final_run_nm,
    _point_in_approach_box,
    _normalize_runway_ident,
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
    assert _score_termination(None) == "none"


def test_score_gap():
    assert _score_gap(2.0) == "green"
    assert _score_gap(4.9) == "green"
    assert _score_gap(5.0) == "yellow"
    assert _score_gap(10.0) == "yellow"
    assert _score_gap(14.9) == "yellow"
    assert _score_gap(15.0) == "red"
    assert _score_gap(30.0) == "red"
    assert _score_gap(None) == "none"


def test_overall_score():
    assert _overall_score("green", "green") == "green"
    assert _overall_score("green", "yellow") == "yellow"
    assert _overall_score("green", "red") == "red"
    assert _overall_score("red", "red") == "red"
    assert _overall_score("yellow", "red") == "red"


def test_overall_score_with_none():
    # Both none -> none
    assert _overall_score("none", "none") == "none"
    # Termination=none drives overall=none regardless of gap signal:
    # without approach data we can't score approach quality, even if
    # en-route gaps look fine.
    assert _overall_score("none", "green") == "none"
    assert _overall_score("none", "yellow") == "none"
    assert _overall_score("none", "red") == "none"
    # Gap=none alone defers to termination (gap=None implies near-zero
    # records, in which case termination would also typically be none).
    assert _overall_score("green", "none") == "green"
    assert _overall_score("yellow", "none") == "yellow"
    assert _overall_score("red", "none") == "red"


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


# --- Runway detection tests ---

# A synthetic runway "36": landing heading 360 (north), threshold at (37, -122).
# Aircraft on final for it approach from the south, tracking north.
SYNTH_BOX_36 = {"ident": "36", "heading": 360.0, "lat": 37.0, "lon": -122.0}
# Approximate degrees of latitude per nm, for placing test points.
_DEG_LAT_PER_NM = 1.0 / 60.0


def _south_of(box, nm):
    """A point `nm` south of the threshold (on the extended centerline)."""
    return (box["lat"] - nm * _DEG_LAT_PER_NM, box["lon"])


def test_point_in_approach_box():
    box = SYNTH_BOX_36
    # 1 nm out on the centerline -> inside.
    lat, lon = _south_of(box, 1.0)
    assert _point_in_approach_box(lat, lon, box)
    # Beyond the 3 nm box length -> outside.
    lat, lon = _south_of(box, 4.0)
    assert not _point_in_approach_box(lat, lon, box)
    # On the wrong side of the threshold (past it) -> outside.
    lat, lon = _south_of(box, -0.5)
    assert not _point_in_approach_box(lat, lon, box)
    # 1 nm out but offset ~0.5 nm laterally (> 1250 ft half width) -> outside.
    lat, lon = _south_of(box, 1.0)
    lon_off = lon + 0.5 * _DEG_LAT_PER_NM / 0.8  # ~0.5 nm east
    assert not _point_in_approach_box(lat, lon_off, box)


def test_approach_runway_final():
    box = SYNTH_BOX_36
    # A sustained run up the centerline (tracking north) -> vote 36.
    pts = [_south_of(box, nm) for nm in (1.2, 1.0, 0.8, 0.5, 0.3)]
    assert _approach_runway(pts, [box]) == "36"


def test_approach_runway_momentary_clip_no_vote():
    """A single short segment clipping the box (a pattern turn) isn't a
    sustained final and must not vote (see MIN_FINAL_RUN_NM)."""
    box = SYNTH_BOX_36
    # One aligned segment ~0.1 nm long, below the run threshold.
    pts = [_south_of(box, 0.6), _south_of(box, 0.5)]
    assert _approach_runway(pts, [box]) is None


def test_longest_final_run_nm():
    box = SYNTH_BOX_36
    # ~0.9 nm of continuous aligned travel up the centerline.
    pts = [_south_of(box, nm) for nm in (1.2, 1.0, 0.8, 0.5, 0.3)]
    assert _longest_final_run_nm(pts, box) == pytest.approx(0.9, abs=0.05)
    # A misaligned/off-centerline pass accumulates no run.
    off = 0.5 * _DEG_LAT_PER_NM / 0.8
    lat, lon = _south_of(box, 1.0)
    assert _longest_final_run_nm([(lat, lon - off), (lat, lon + off)], box) == 0.0


def test_runway_votes_split_on_gap():
    """Repeated approaches separated by a time gap each vote once.

    These approaches stay >CROSS_NEAR_NM from the field (closest 0.5 nm) so
    they don't also trigger the field-crossing split — this isolates the
    time-gap behavior.
    """
    box = SYNTH_BOX_36
    approach = [_south_of(box, nm) for nm in (1.4, 1.2, 1.0, 0.7, 0.5)]
    t0 = 1000
    v1 = [(t0 + i, la, lo) for i, (la, lo) in enumerate(approach)]
    t1 = t0 + 100 + APPROACH_SPLIT_GAP_S + 1
    v2 = [(t1 + i, la, lo) for i, (la, lo) in enumerate(approach)]
    assert runway_votes_for_track(v1 + v2, [box]) == ["36", "36"]
    # Continuous, same geometry, no field overpass -> a single vote.
    cont = [(t0 + i, la, lo) for i, (la, lo) in enumerate(approach)]
    assert runway_votes_for_track(cont, [box]) == ["36"]


def test_runway_votes_split_on_field_crossing():
    """A continuous touch-and-go (no time gap) splits at the field overpass.

    Fly up the centerline and over the field (within CROSS_NEAR_NM), climb out
    the far side past CROSS_FAR_NM, then come back for a second approach — two
    votes despite one continuous track.
    """
    box = SYNTH_BOX_36
    # First approach up to and over the field, then out the north side.
    def north_of(nm):
        return (box["lat"] + nm * _DEG_LAT_PER_NM, box["lon"])
    approach1 = [_south_of(box, nm) for nm in (1.4, 1.0, 0.6, 0.2)]  # crosses field
    depart1 = [north_of(nm) for nm in (0.5, 1.2)]                    # out past CROSS_FAR_NM
    approach2 = [_south_of(box, nm) for nm in (1.4, 1.0, 0.6, 0.2)]  # second approach
    seq = approach1 + depart1 + approach2
    cont = [(1000 + i, la, lo) for i, (la, lo) in enumerate(seq)]
    assert runway_votes_for_track(cont, [box]) == ["36", "36"]


def test_approach_runway_downwind_no_vote():
    """A downwind leg (flying south, opposite the landing direction) must not
    vote for the reciprocal runway, even if it clips the box area."""
    box = SYNTH_BOX_36
    # Points near the centerline but ordered north->south (heading ~180).
    pts = [_south_of(box, nm) for nm in (0.3, 0.5, 0.8, 1.0, 1.2)]
    assert _approach_runway(pts, [box]) is None


def test_approach_runway_offset_no_vote():
    """A base leg / off-centerline pass casts no vote."""
    box = SYNTH_BOX_36
    lat1, lon1 = _south_of(box, 1.0)
    lat2, lon2 = _south_of(box, 1.0)
    # Two points well off to the side, crossing east->west (base leg).
    off = 0.5 * _DEG_LAT_PER_NM / 0.8
    pts = [(lat1, lon1 - off), (lat2, lon2 + off)]
    assert _approach_runway(pts, [box]) is None


def test_normalize_runway_ident():
    # Parallel runways collapse to the bare number.
    assert _normalize_runway_ident("09L") == "09"
    assert _normalize_runway_ident("09R") == "09"
    assert _normalize_runway_ident("27C") == "27"
    assert _normalize_runway_ident("27") == "27"
    assert _normalize_runway_ident(" 02 ") == "02"
    # Single-digit idents zero-pad so "9" and "09" don't split votes.
    assert _normalize_runway_ident("9") == "09"
    assert _normalize_runway_ident("5L") == "05"
    # Non-runway ends OurAirports also lists -> "" (skipped by callers).
    assert _normalize_runway_ident("H3") == ""     # helipad
    assert _normalize_runway_ident("NE") == ""     # compass-named grass strip
    assert _normalize_runway_ident("SWL") == ""
    assert _normalize_runway_ident("37") == ""     # out of 01-36 range
    assert _normalize_runway_ident("00") == ""


def test_build_runway_boxes_kwvi():
    """KWVI has runways 02/20 and 09/27 in OurAirports (from local cache)."""
    boxes = {b["ident"]: b for b in build_runway_boxes("KWVI")}
    assert set(boxes) == {"02", "20", "09", "27"}
    # Headings are true degrees, roughly matching the magnetic idents.
    assert abs(boxes["02"]["heading"] - 20) < 20
    assert abs(boxes["27"]["heading"] - 270) < 20
    # Thresholds are near the airport.
    assert abs(boxes["02"]["lat"] - KWVI_LAT) < 0.1
    assert abs(boxes["02"]["lon"] - KWVI_LON) < 0.1


def test_summarize_runway_usage():
    usage = _summarize_runway_usage({"20": 3, "02": 1})
    assert usage == [{"runway": "20", "pct": 75}, {"runway": "02", "pct": 25}]
    # Sorted descending, zero-count runways absent, empty input -> [].
    assert _summarize_runway_usage({}) == []


def test_build_data_quality_runways(tmp_path):
    """Runway usage is detected and reported for the KWVI fixtures."""
    airport_dir = tmp_path / "KWVI"
    airport_dir.mkdir()
    for f in FIXTURES.iterdir():
        shutil.copy2(f, airport_dir / f.name)

    result = build_data_quality("KWVI", airport_dir,
                                field_elev=KWVI_ELEV,
                                airport_lat=KWVI_LAT,
                                airport_lon=KWVI_LON)

    usage = result["runwayUsage"]
    assert usage, "expected some runway usage votes"
    # Sorted descending by pct, and pcts sum to ~100.
    pcts = [u["pct"] for u in usage]
    assert pcts == sorted(pcts, reverse=True)
    assert abs(sum(pcts) - 100) <= 1
    # Every reported runway is a real KWVI runway.
    valid = {"02", "20", "09", "27"}
    assert all(u["runway"] in valid for u in usage)
    # runwayCounts totals should be consistent with the usage entries.
    counts = result["runwayCounts"]
    assert set(counts) == {u["runway"] for u in usage}


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
    assert details["terminationScore"] in ("green", "yellow", "red", "none")
    assert details["gapScore"] in ("green", "yellow", "red", "none")

    # Gap metrics should be present
    assert result["medianGapS"] is not None
    assert result["medianGapS"] > 0


def test_build_data_quality_empty(tmp_path):
    """Empty directory returns None (airport not evaluated at all)."""
    airport_dir = tmp_path / "EMPTY"
    airport_dir.mkdir()
    result = build_data_quality("EMPTY", airport_dir, field_elev=0,
                                airport_lat=0.0, airport_lon=0.0)
    assert result is None


def test_aggregate_per_date_results_no_data():
    """Evaluated but all dates yielded no usable data → score 'none' dict."""
    from src.tools.data_quality import aggregate_per_date_results
    result = aggregate_per_date_results([None, None, None],
                                        "FAKE", num_dates=3)
    assert result is not None
    assert result["icao"] == "FAKE"
    assert result["score"] == "none"
    assert result["numDates"] == 3
    assert result["lostRate"] is None
    assert result["medianGapS"] is None
    assert result["totalLowAltTracks"] == 0


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


def test_runway_usage_in_tooltip():
    """The quality badge tooltip lists per-runway landing percentages."""
    import json
    from src.lib.map_elements import build_quality_indicator_json

    quality = {
        "score": "green",
        "completionRate": 0.9,
        "medianGapS": 3.0,
        "numDates": 3,
        "runwayUsage": [{"runway": "20", "pct": 60},
                        {"runway": "02", "pct": 40}],
    }
    indicator = json.loads(build_quality_indicator_json(quality))
    assert "Percent of landings by runway" in indicator["tooltip"]
    assert "20: 60%" in indicator["tooltip"]
    assert "02: 40%" in indicator["tooltip"]
