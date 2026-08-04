"""Integration tests for the busyness chart feature.

Uses pre-fetched KWVI fixtures (3 weekend dates + METAR cache) so no
network access is needed. Tests the full pipeline: traffic counting,
METAR classification, aggregation, and HTML chart rendering.
"""

import json
import shutil
from pathlib import Path

import pytest

from src.tools.metar_history import (
    classify_flight_category,
    icao_to_faa_lid,
    parse_metar_csv,
)
from src.tools.busyness import (
    build_busyness_data,
    count_hourly_traffic,
    parse_date_from_shard,
)

FIXTURES = Path("tests/fixtures/KWVI")


# --- Unit tests for metar_history ---

def test_icao_to_faa_lid():
    assert icao_to_faa_lid("KWVI") == "WVI"
    assert icao_to_faa_lid("KCCB") == "CCB"
    assert icao_to_faa_lid("E16") == "E16"
    assert icao_to_faa_lid("PHNL") == "PHNL"


def test_classify_flight_category():
    # VFR: vis > 5 AND ceiling > 3000 (or no ceiling)
    assert classify_flight_category(10.0, None) == "VFR"
    assert classify_flight_category(10.0, 5000) == "VFR"

    # MVFR: vis 3-5 OR ceiling 1000-3000
    assert classify_flight_category(4.0, None) == "MVFR"
    assert classify_flight_category(10.0, 2500) == "MVFR"

    # IFR: vis 1-3 OR ceiling 500-1000
    assert classify_flight_category(2.0, None) == "IFR"
    assert classify_flight_category(10.0, 800) == "IFR"

    # LIFR: vis < 1 OR ceiling < 500
    assert classify_flight_category(0.5, None) == "LIFR"
    assert classify_flight_category(10.0, 200) == "LIFR"

    # Worse of ceiling and visibility wins
    assert classify_flight_category(0.5, 5000) == "LIFR"  # vis is LIFR
    assert classify_flight_category(10.0, 200) == "LIFR"  # ceiling is LIFR

    # Unknown
    assert classify_flight_category(None, None) == "UNKNOWN"


def test_parse_metar_csv():
    csv_text = FIXTURES / "KWVI_metar_2025.csv"
    assert csv_text.exists(), f"Missing fixture: {csv_text}"

    categories = parse_metar_csv(csv_text.read_text())
    assert len(categories) > 0

    # June 1 afternoon in Watsonville should be VFR
    # Check a few hours — at least some should be VFR
    vfr_count = sum(1 for (d, h), cat in categories.items()
                    if d == "2025-06-01" and cat == "VFR")
    assert vfr_count > 0, "Expected some VFR hours on June 1"


# --- Unit tests for busyness ---

def test_parse_date_from_shard():
    assert parse_date_from_shard("060125_KWVI.gz") == "2025-06-01"
    assert parse_date_from_shard("123124_KCCB.gz") == "2024-12-31"
    assert parse_date_from_shard("bad_name.gz") is None


def test_count_hourly_traffic():
    shard = FIXTURES / "060125_KWVI.gz"
    assert shard.exists(), f"Missing fixture: {shard}"

    hourly = count_hourly_traffic(shard, field_elev=159)
    assert len(hourly) > 0, "Should have traffic in at least one hour"
    assert all(isinstance(v, int) and v > 0 for v in hourly.values())


# --- Integration test ---

def test_build_busyness_data(tmp_path):
    """Full integration: build busyness data from fixtures."""
    # Copy fixtures to temp dir (so we don't modify originals)
    airport_dir = tmp_path / "KWVI"
    airport_dir.mkdir()
    for f in FIXTURES.iterdir():
        shutil.copy2(f, airport_dir / f.name)

    result = build_busyness_data("KWVI", airport_dir,
                                 metar_year=2025,
                                 metar_cache_dir=airport_dir,
                                 field_elev=159)

    assert result is not None
    assert result["icao"] == "KWVI"
    assert result["numDates"] == 3
    assert result["hasWeather"] is True
    assert result["globalMax"] > 0
    assert "VMC" in result["weatherCategories"]

    # All 3 dates are weekends, so weekend buckets should have data
    data = result["data"]
    weekend_keys = [k for k in data if ":weekend:" in k]
    assert len(weekend_keys) > 0, "Should have weekend data"

    # Weekday should have no data (all dates are Sat/Sun)
    weekday_keys = [k for k in data if ":weekday:" in k and ":ALL" not in k]
    # weekday keys might exist from the "all" day_type bucket, but
    # specific weekday+weather combos should be empty
    weekday_specific = [k for k in data
                        if k.split(":")[1] == "weekday"
                        and k.split(":")[2] != "ALL"]
    assert len(weekday_specific) == 0, "No weekday+weather data expected"


def test_busyness_html_rendering(tmp_path):
    """Test that the visualizer produces HTML with chart elements."""
    # Build busyness data
    airport_dir = tmp_path / "KWVI"
    airport_dir.mkdir()
    for f in FIXTURES.iterdir():
        shutil.copy2(f, airport_dir / f.name)

    busyness_data = build_busyness_data("KWVI", airport_dir,
                                        metar_year=2025,
                                        metar_cache_dir=airport_dir,
                                        field_elev=159)
    assert busyness_data is not None

    # Write busyness JSON
    busyness_json = airport_dir / "KWVI_busyness.json"
    busyness_json.write_text(json.dumps(busyness_data))

    # Combine CSV files for visualizer stdin
    combined_csv = airport_dir / "KWVI_combined.csv.out"
    csv_content = ""
    for f in sorted(airport_dir.glob("*_KWVI.csv.out")):
        csv_content += f.read_text()
    combined_csv.write_text(csv_content)

    # Combine traffic files
    combined_traffic = airport_dir / "KWVI_traffic_combined.csv"
    traffic_content = ""
    for f in sorted(airport_dir.glob("*_KWVI_traffic.csv")):
        traffic_content += f.read_text()
    combined_traffic.write_text(traffic_content)

    # Run visualizer
    import subprocess
    output_html = airport_dir / "test_map.html"
    result = subprocess.run(
        [
            "python", "src/postprocessing/visualizer.py",
            "--sw", "36.85,-121.87",
            "--ne", "36.97,-121.72",
            "--traffic-samples", str(combined_traffic),
            "--busyness-data", str(busyness_json),
            "--output", str(output_html),
            "--no-browser",
        ],
        input=csv_content,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"Visualizer failed: {result.stderr}"
    assert output_html.exists()

    html = output_html.read_text()
    assert "busyness-chart" in html
    assert "chart.js" in html.lower() or "Chart(" in html
    assert "busyness-panel" in html
    assert "busy-btn" in html
    assert '"globalMax"' in html

    # Verify weather filter buttons are present (since KWVI has METAR data)
    assert "weather-btn" in html
    assert "VMC" in html

    print(f"\nOutput HTML: {output_html}")
