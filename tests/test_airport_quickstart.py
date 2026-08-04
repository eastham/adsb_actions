"""Tests for the airport_quickstart script."""

import sys
from pathlib import Path

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "tools"))

from airport_quickstart import (
    parse_wind_from_metar,
    select_wind_favored_runway,
)


class TestParseWindFromMetar:
    """Test METAR wind parsing."""

    def test_parse_normal_wind(self):
        """Test parsing standard wind report."""
        direction, speed = parse_wind_from_metar("KOAK 251353Z 31008KT 10SM CLR")
        assert direction == 310
        assert speed == 8

    def test_parse_wind_with_gusts(self):
        """Test parsing wind with gusts."""
        direction, speed = parse_wind_from_metar("KSFO 251356Z 27015G25KT 10SM FEW020")
        assert direction == 270
        assert speed == 15

    def test_parse_variable_wind(self):
        """Test parsing variable wind."""
        direction, speed = parse_wind_from_metar("KPAO 251350Z VRB05KT 10SM CLR")
        assert direction is None
        assert speed == 5

    def test_parse_calm_wind(self):
        """Test parsing calm wind."""
        direction, speed = parse_wind_from_metar("KSQL 251350Z 00000KT 10SM CLR")
        assert direction is None
        assert speed == 0

    def test_parse_no_wind_data(self):
        """Test parsing METAR without wind data."""
        direction, speed = parse_wind_from_metar("KOAK 251353Z 10SM CLR")
        assert direction is None
        assert speed == 0

    def test_parse_three_digit_speed(self):
        """Test parsing wind with 3-digit speed (>=100kt)."""
        direction, speed = parse_wind_from_metar("KDEN 251350Z 270100KT 10SM CLR")
        assert direction == 270
        assert speed == 100


class TestSelectWindFavoredRunway:
    """Test wind-favored runway selection."""

    @pytest.fixture
    def sample_runways(self):
        """Sample runway data for testing."""
        return [
            {
                'le_ident': '12',
                'he_ident': '30',
                'le_heading_degT': '120',
                'he_heading_degT': '300',
            },
            {
                'le_ident': '06',
                'he_ident': '24',
                'le_heading_degT': '060',
                'he_heading_degT': '240',
            },
        ]

    @pytest.fixture
    def parallel_runways(self):
        """Parallel runway data (like KSFO)."""
        return [
            {
                'le_ident': '10L',
                'he_ident': '28R',
                'le_heading_degT': '100',
                'he_heading_degT': '280',
            },
            {
                'le_ident': '10R',
                'he_ident': '28L',
                'le_heading_degT': '100',
                'he_heading_degT': '280',
            },
        ]

    def test_select_direct_headwind(self, sample_runways):
        """Test selection with wind directly aligned with runway."""
        # Wind from 300, should select runway 30 (heading 300)
        result = select_wind_favored_runway(300, sample_runways)
        assert result is not None
        runway, ident = result
        assert ident == '30'

    def test_select_slight_crosswind(self, sample_runways):
        """Test selection with slight crosswind component."""
        # Wind from 290, still closest to runway 30 (heading 300)
        result = select_wind_favored_runway(290, sample_runways)
        assert result is not None
        runway, ident = result
        assert ident == '30'

    def test_select_opposite_runway(self, sample_runways):
        """Test selection when wind favors opposite end."""
        # Wind from 120, should select runway 12 (heading 120)
        result = select_wind_favored_runway(120, sample_runways)
        assert result is not None
        runway, ident = result
        assert ident == '12'

    def test_select_different_runway(self, sample_runways):
        """Test selection of a different runway entirely."""
        # Wind from 060, should select runway 06 (heading 060)
        result = select_wind_favored_runway(60, sample_runways)
        assert result is not None
        runway, ident = result
        assert ident == '06'

    def test_select_with_crosswind(self, sample_runways):
        """Test selection when wind is between runways."""
        # Wind from 090 - between 06 (060) and 12 (120)
        # Both are 30 degrees off, but algorithm will pick one
        result = select_wind_favored_runway(90, sample_runways)
        assert result is not None
        runway, ident = result
        # Either 06 or 12 is acceptable (they're equidistant)
        assert ident in ['06', '12']

    def test_select_parallel_runways(self, parallel_runways):
        """Test selection with parallel runways (should pick one)."""
        # Wind from 280, either 28L or 28R is acceptable
        result = select_wind_favored_runway(280, parallel_runways)
        assert result is not None
        runway, ident = result
        assert ident in ['28L', '28R']

    def test_select_none_wind_direction(self, sample_runways):
        """Test that None wind direction returns None."""
        result = select_wind_favored_runway(None, sample_runways)
        assert result is None

    def test_select_wraparound_north(self, sample_runways):
        """Test selection when wind is near 360/0 degrees."""
        # Add a north-south runway
        runways = sample_runways + [{
            'le_ident': '36',
            'he_ident': '18',
            'le_heading_degT': '360',
            'he_heading_degT': '180',
        }]
        # Wind from 350, should favor runway 36 (heading 360)
        result = select_wind_favored_runway(350, runways)
        assert result is not None
        runway, ident = result
        assert ident == '36'

    def test_select_wraparound_from_010(self):
        """Test selection with wind from 010 degrees."""
        runways = [{
            'le_ident': '36',
            'he_ident': '18',
            'le_heading_degT': '360',
            'he_heading_degT': '180',
        }]
        # Wind from 010, should favor runway 36 (heading 360) - only 10 degrees off
        result = select_wind_favored_runway(10, runways)
        assert result is not None
        runway, ident = result
        assert ident == '36'

    def test_empty_runways(self):
        """Test with empty runway list."""
        result = select_wind_favored_runway(270, [])
        assert result is None

    def test_missing_heading_data(self):
        """Test handling of runways with missing heading data."""
        runways = [
            {
                'le_ident': '12',
                'he_ident': '30',
                # Missing heading data
            },
        ]
        result = select_wind_favored_runway(300, runways)
        # Should return None since no valid headings
        assert result is None

    def test_real_world_koak(self):
        """Test with real KOAK runway configuration."""
        # KOAK has runways 10L/28R, 10R/28L, 12/30, and others
        runways = [
            {
                'le_ident': '10L',
                'he_ident': '28R',
                'le_heading_degT': '105',
                'he_heading_degT': '285',
            },
            {
                'le_ident': '10R',
                'he_ident': '28L',
                'le_heading_degT': '105',
                'he_heading_degT': '285',
            },
            {
                'le_ident': '12',
                'he_ident': '30',
                'le_heading_degT': '115',
                'he_heading_degT': '295',
            },
        ]
        # Typical SF Bay Area wind from 290
        result = select_wind_favored_runway(290, runways)
        assert result is not None
        runway, ident = result
        # Should pick one of the 28s or 30 (all close to 290)
        assert ident in ['28R', '28L', '30']
