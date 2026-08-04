"""Tests for the batch_los_pipeline script."""

import sys
import tempfile
from datetime import datetime
from pathlib import Path

# Add src and src/tools to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "tools"))

from tools.batch_los_pipeline import (
    generate_multi_airport_yaml,
    ESTIMATED_DAILY_DATA_GB,
    ANALYSIS_RADIUS_NM,
)
from tools.batch_helpers import (
    faa_to_icao,
    generate_date_range,
    is_weekend,
    estimate_download_size,
    compute_bounds,
    print_pipeline_summary,
    print_completion_summary,
    load_airport_list,
    derive_heatmap_label,
)


class TestFaaToIcao:
    """Test FAA to ICAO code conversion."""

    def test_three_letter_code(self):
        """Test standard 3-letter FAA code."""
        assert faa_to_icao("DCU") == "KDCU"
        assert faa_to_icao("EUL") == "KEUL"
        assert faa_to_icao("LGU") == "KLGU"

    def test_alphanumeric_code(self):
        """Test codes with numbers stay as-is (no K prefix)."""
        assert faa_to_icao("1R8") == "1R8"
        assert faa_to_icao("2R4") == "2R4"
        assert faa_to_icao("71J") == "71J"

    def test_lowercase_input(self):
        """Test that lowercase is converted to uppercase."""
        assert faa_to_icao("dcu") == "KDCU"
        assert faa_to_icao("eul") == "KEUL"

    def test_already_icao(self):
        """Test that already ICAO code is returned as-is."""
        assert faa_to_icao("KDCU") == "KDCU"
        assert faa_to_icao("KSQL") == "KSQL"

    def test_whitespace_handling(self):
        """Test that whitespace is stripped."""
        assert faa_to_icao(" DCU ") == "KDCU"
        assert faa_to_icao("  1R8  ") == "1R8"


class TestLoadAirportList:
    """Test airport list file parsing."""

    def test_simple_format(self):
        """Test parsing simple one-per-line format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("DCU\n")
            f.write("EUL\n")
            f.write("1R8\n")
            filepath = f.name
        try:
            airports, overrides = load_airport_list(filepath)
            assert airports == ["DCU", "EUL", "1R8"]
            assert overrides == {}
        finally:
            Path(filepath).unlink()

    def test_empty_lines_ignored(self):
        """Test that empty lines are skipped."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("DCU\n")
            f.write("\n")
            f.write("EUL\n")
            f.write("   \n")
            f.write("1R8\n")
            filepath = f.name
        try:
            airports, overrides = load_airport_list(filepath)
            assert airports == ["DCU", "EUL", "1R8"]
            assert overrides == {}
        finally:
            Path(filepath).unlink()

    def test_max_airports(self):
        """Test limiting to max_airports."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("DCU\nEUL\n1R8\nBDN\nLGU\n")
            filepath = f.name
        try:
            airports, overrides = load_airport_list(filepath, max_airports=3)
            assert airports == ["DCU", "EUL", "1R8"]
        finally:
            Path(filepath).unlink()

    def test_real_file(self):
        """Test parsing the actual busiest_nontowered.txt file."""
        filepath = Path(__file__).parent.parent / "examples" / "busiest_nontowered.txt"
        if filepath.exists():
            airports, overrides = load_airport_list(str(filepath), max_airports=5)
            assert len(airports) == 5
            assert airports[0] == "DCU"  # First airport in the list

    def test_radius_override(self):
        """Test parsing per-airport radius overrides."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("DCU\nKEIK 7\nEUL 5.5\n")
            filepath = f.name
        try:
            airports, overrides = load_airport_list(filepath)
            assert airports == ["DCU", "KEIK", "EUL"]
            assert "DCU" not in overrides
            assert overrides["KEIK"] == 7.0
            assert overrides["EUL"] == 5.5
        finally:
            Path(filepath).unlink()

    def test_radius_override_trimmed_by_max_airports(self):
        """Test that radius overrides respect max_airports limit."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("DCU\nKEIK 7\nEUL 5.5\n")
            filepath = f.name
        try:
            airports, overrides = load_airport_list(filepath, max_airports=1)
            assert airports == ["DCU"]
            assert overrides == {}
        finally:
            Path(filepath).unlink()


class TestIsWeekend:
    """Test weekend detection."""

    def test_saturday(self):
        """Test Saturday is detected as weekend."""
        sat = datetime(2026, 1, 17)  # Saturday
        assert is_weekend(sat) is True

    def test_sunday(self):
        """Test Sunday is detected as weekend."""
        sun = datetime(2026, 1, 18)  # Sunday
        assert is_weekend(sun) is True

    def test_weekdays(self):
        """Test weekdays are not weekend."""
        mon = datetime(2026, 1, 12)  # Monday
        tue = datetime(2026, 1, 13)  # Tuesday
        wed = datetime(2026, 1, 14)  # Wednesday
        thu = datetime(2026, 1, 15)  # Thursday
        fri = datetime(2026, 1, 16)  # Friday

        assert is_weekend(mon) is False
        assert is_weekend(tue) is False
        assert is_weekend(wed) is False
        assert is_weekend(thu) is False
        assert is_weekend(fri) is False


class TestGenerateDateRange:
    """Test date range generation with filtering."""

    def test_all_dates(self):
        """Test generating all dates in range."""
        start = datetime(2026, 1, 15)
        end = datetime(2026, 1, 17)
        dates = generate_date_range(start, end, 'all')
        assert len(dates) == 3
        assert dates[0] == datetime(2026, 1, 15)
        assert dates[1] == datetime(2026, 1, 16)
        assert dates[2] == datetime(2026, 1, 17)

    def test_single_date(self):
        """Test single-day range."""
        date = datetime(2026, 1, 15)
        dates = generate_date_range(date, date, 'all')
        assert len(dates) == 1
        assert dates[0] == date

    def test_weekday_filter(self):
        """Test filtering to weekdays only."""
        # Mon Jan 12 through Sun Jan 18
        start = datetime(2026, 1, 12)
        end = datetime(2026, 1, 18)
        dates = generate_date_range(start, end, 'weekday')
        # Should have Mon, Tue, Wed, Thu, Fri (5 days)
        assert len(dates) == 5
        for d in dates:
            assert d.weekday() < 5  # Monday=0 through Friday=4

    def test_weekend_filter(self):
        """Test filtering to weekends only."""
        # Mon Jan 12 through Sun Jan 18
        start = datetime(2026, 1, 12)
        end = datetime(2026, 1, 18)
        dates = generate_date_range(start, end, 'weekend')
        # Should have Sat Jan 17 and Sun Jan 18
        assert len(dates) == 2
        assert dates[0] == datetime(2026, 1, 17)  # Saturday
        assert dates[1] == datetime(2026, 1, 18)  # Sunday

    def test_no_matching_dates(self):
        """Test when filter excludes all dates."""
        # Weekdays only, but range is Sat-Sun
        start = datetime(2026, 1, 17)  # Saturday
        end = datetime(2026, 1, 18)    # Sunday
        dates = generate_date_range(start, end, 'weekday')
        assert len(dates) == 0

    def test_week_range(self):
        """Test a full week range."""
        # Full week Mon-Sun
        start = datetime(2026, 1, 12)
        end = datetime(2026, 1, 18)
        dates = generate_date_range(start, end, 'all')
        assert len(dates) == 7


class TestComputeBounds:
    """Test SW/NE bounds computation."""

    def test_basic_bounds(self):
        """Test bounds are symmetric around center."""
        sw_lat, sw_lon, ne_lat, ne_lon = compute_bounds(37.0, -122.0, 5.0)
        # Latitude offset = 5/60 = 0.0833 degrees
        assert abs(ne_lat - 37.0 - 5.0/60.0) < 0.001
        assert abs(37.0 - sw_lat - 5.0/60.0) < 0.001
        # NE should be greater than SW
        assert ne_lat > sw_lat
        assert ne_lon > sw_lon

    def test_zero_radius(self):
        """Test zero radius returns center point."""
        sw_lat, sw_lon, ne_lat, ne_lon = compute_bounds(37.0, -122.0, 0.0)
        assert abs(sw_lat - 37.0) < 0.001
        assert abs(ne_lat - 37.0) < 0.001


class TestGenerateMultiAirportYaml:
    """Test multi-airport shard YAML generation."""

    def test_single_airport(self):
        """Test YAML generation for one airport."""
        airport_info = {"KDCU": (34.6527, -86.9453, 592)}
        yaml = generate_multi_airport_yaml(["KDCU"], "011526", airport_info)

        assert "rules:" in yaml
        assert "KDCU_shard:" in yaml
        assert "latlongring:" in yaml
        assert "34.6527" in yaml
        assert "-86.9453" in yaml
        assert "emit_jsonl:" in yaml
        assert "011526_KDCU.gz" in yaml

    def test_multiple_airports(self):
        """Test YAML generation for multiple airports."""
        airport_info = {
            "KDCU": (34.6527, -86.9453, 592),
            "KEUL": (43.6427, -116.6363, 2537),
        }
        yaml = generate_multi_airport_yaml(
            ["KDCU", "KEUL"], "011526", airport_info)

        assert "KDCU_shard:" in yaml
        assert "KEUL_shard:" in yaml
        assert "011526_KDCU.gz" in yaml
        assert "011526_KEUL.gz" in yaml

    def test_yaml_structure(self):
        """Test YAML has correct structure for adsb_actions parsing."""
        airport_info = {"KSQL": (37.5072, -122.2497, 5)}
        yaml = generate_multi_airport_yaml(["KSQL"], "011526", airport_info)

        lines = yaml.strip().split("\n")
        # Should have: comment, rules:, rule_name:, conditions:, latlongring:, actions:, emit_jsonl:
        assert lines[1] == "rules:"
        assert "KSQL_shard:" in lines[2]
        assert "conditions:" in lines[3]
        assert "latlongring:" in lines[4]
        assert "actions:" in lines[5]
        assert "emit_jsonl:" in lines[6]

    def test_radius_matches_constant(self):
        """Test that the radius in YAML matches ANALYSIS_RADIUS_NM."""
        airport_info = {"KDCU": (34.6527, -86.9453, 592)}
        yaml = generate_multi_airport_yaml(["KDCU"], "011526", airport_info)
        assert f"[{ANALYSIS_RADIUS_NM}," in yaml


class TestDownloadEstimate:
    """Test download size estimation."""

    def test_check_cached_dates_none_cached(self):
        """Test when no dates are cached."""
        from pathlib import Path
        dates = [datetime(2099, 1, 15), datetime(2099, 1, 16)]
        data_dir = Path("data")  # Using default data directory
        estimated_gb, cached, uncached = estimate_download_size(dates, data_dir, ESTIMATED_DAILY_DATA_GB)
        assert len(cached) == 0
        assert len(uncached) == 2

    def test_estimate_download_size(self):
        """Test download size estimation."""
        from pathlib import Path
        dates = [datetime(2099, 1, 15), datetime(2099, 1, 16), datetime(2099, 1, 17)]
        data_dir = Path("data")
        estimated_gb, cached, uncached = estimate_download_size(dates, data_dir, ESTIMATED_DAILY_DATA_GB)
        # All dates should be uncached (future dates)
        assert len(cached) == 0
        assert len(uncached) == 3
        assert estimated_gb == 3 * ESTIMATED_DAILY_DATA_GB

    def test_estimate_with_no_dates(self):
        """Test estimation with empty date list."""
        from pathlib import Path
        data_dir = Path("data")
        estimated_gb, cached, uncached = estimate_download_size([], data_dir, ESTIMATED_DAILY_DATA_GB)
        assert estimated_gb == 0
        assert len(cached) == 0
        assert len(uncached) == 0


class TestIntegration:
    """Integration tests combining multiple functions."""

    def test_full_workflow_dry_run(self):
        """Test the full workflow logic with mocked file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            f.write("DCU\nEUL\n1R8\n")
            filepath = f.name

        try:
            # Load airports
            faa_codes, _ = load_airport_list(filepath, max_airports=2)
            icao_codes = [faa_to_icao(code) for code in faa_codes]
            assert icao_codes == ["KDCU", "KEUL"]

            # Generate dates (single day)
            dates = generate_date_range(
                datetime(2026, 1, 15),
                datetime(2026, 1, 15),
                'all'
            )
            assert len(dates) == 1

            # Generate shard YAML
            airport_info = {
                "KDCU": (34.6527, -86.9453, 592),
                "KEUL": (43.6427, -116.6363, 2537),
            }
            yaml = generate_multi_airport_yaml(
                icao_codes, "011526", airport_info)
            assert "KDCU_shard:" in yaml
            assert "KEUL_shard:" in yaml

        finally:
            Path(filepath).unlink()


class TestDeriveHeatmapLabel:
    """Test derive_heatmap_label date-range label generation."""

    def _paths(self, *stems):
        """Build fake Path objects from MMDDYY_ICAO stem strings."""
        return [Path(f"{s}.csv.out") for s in stems]

    def test_single_date(self):
        files = self._paths("060125_KWVI")
        assert derive_heatmap_label(files) == "6/1/25"

    def test_contiguous_range_no_outliers(self):
        files = self._paths("060125_KWVI", "060225_KWVI", "060325_KWVI")
        assert derive_heatmap_label(files) == "6/1/25 - 6/3/25"

    def test_contiguous_range_one_outlier(self):
        # 6/1-6/3 is the main run; 6/10 is an outlier
        files = self._paths("060125_KWVI", "060225_KWVI", "060325_KWVI", "061025_KWVI")
        assert derive_heatmap_label(files) == "6/1/25 - 6/3/25 + 1 other date"

    def test_contiguous_range_two_outliers(self):
        files = self._paths(
            "060125_KWVI", "060225_KWVI", "060325_KWVI",
            "061025_KWVI", "062025_KWVI"
        )
        assert derive_heatmap_label(files) == "6/1/25 - 6/3/25 + 2 other dates"

    def test_two_runs_picks_longer(self):
        # Run A: 6/1-6/3 (3 days), Run B: 6/10 only — A wins
        files = self._paths("060125_KWVI", "060225_KWVI", "060325_KWVI", "061025_KWVI")
        label = derive_heatmap_label(files)
        assert label.startswith("6/1/25 - 6/3/25")

    def test_no_parseable_dates(self):
        files = [Path("KWVI_combined.csv.out"), Path("some_other_file.csv.out")]
        assert derive_heatmap_label(files) == ""

    def test_empty_list(self):
        assert derive_heatmap_label([]) == ""
