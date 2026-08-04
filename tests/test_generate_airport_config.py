"""Tests for the generate_airport_config script."""

import sys
import tempfile
from pathlib import Path

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tools.generate_airport_config import (
    load_airport,
    load_runways,
    get_longest_runway,
    get_lower_runway_end,
    get_runway_end_data,
    generate_circle_polygon,
    generate_wedge_polygon,
    format_heading_range,
    point_at_distance_bearing,
    nm_to_deg_lat,
)


class TestAirportLookup:
    """Test airport and runway data lookup."""

    def test_load_airport_ksql(self):
        """Test loading San Carlos Airport."""
        airport = load_airport("KSQL")
        assert airport is not None
        assert airport['ident'] == "KSQL"
        assert "San Carlos" in airport['name']
        assert float(airport['elevation_ft']) < 100  # Near sea level

    def test_load_airport_kase(self):
        """Test loading high-elevation airport (Aspen)."""
        airport = load_airport("KASE")
        assert airport is not None
        assert float(airport['elevation_ft']) > 7000  # High elevation

    def test_load_airport_international(self):
        """Test loading international airport (Heathrow)."""
        airport = load_airport("EGLL")
        assert airport is not None
        assert "Heathrow" in airport['name']

    def test_load_airport_not_found(self):
        """Test that nonexistent airport returns None."""
        airport = load_airport("XXXX")
        assert airport is None

    def test_load_runways_ksql(self):
        """Test loading runways for KSQL."""
        runways = load_runways("KSQL")
        assert len(runways) >= 1
        # KSQL has runway 12/30
        rwy = runways[0]
        assert rwy['le_ident'] in ['12', '30'] or rwy['he_ident'] in ['12', '30']


class TestRunwaySelection:
    """Test runway selection logic."""

    def test_get_longest_runway(self):
        """Test finding longest runway."""
        runways = [
            {'length_ft': '3000', 'le_ident': '09', 'he_ident': '27'},
            {'length_ft': '5000', 'le_ident': '18', 'he_ident': '36'},
            {'length_ft': '2500', 'le_ident': '04', 'he_ident': '22'},
        ]
        longest = get_longest_runway(runways)
        assert longest['le_ident'] == '18'

    def test_get_longest_runway_empty(self):
        """Test with no runways."""
        assert get_longest_runway([]) is None

    def test_get_lower_runway_end(self):
        """Test selecting lower-numbered runway end."""
        runway = {'le_ident': '12', 'he_ident': '30'}
        assert get_lower_runway_end(runway) == '12'

        runway = {'le_ident': '27', 'he_ident': '09'}
        assert get_lower_runway_end(runway) == '09'

    def test_get_lower_runway_end_with_suffix(self):
        """Test with L/R/C suffixes."""
        runway = {'le_ident': '09L', 'he_ident': '27R'}
        assert get_lower_runway_end(runway) == '09L'

    def test_get_runway_end_data(self):
        """Test extracting runway end data."""
        runway = {
            'le_ident': '12',
            'he_ident': '30',
            'le_latitude_deg': '37.5145',
            'le_longitude_deg': '-122.253',
            'le_heading_degT': '138',
            'le_elevation_ft': '5',
            'he_latitude_deg': '37.5092',
            'he_longitude_deg': '-122.247',
            'he_heading_degT': '318',
            'he_elevation_ft': '5',
            'width_ft': '75',
        }

        end_12 = get_runway_end_data(runway, '12')
        assert end_12 is not None
        assert end_12['ident'] == '12'
        assert abs(end_12['lat'] - 37.5145) < 0.001
        assert abs(end_12['heading'] - 138) < 1

        end_30 = get_runway_end_data(runway, '30')
        assert end_30 is not None
        assert end_30['ident'] == '30'
        assert abs(end_30['heading'] - 318) < 1

    def test_get_runway_end_data_not_found(self):
        """Test with invalid runway identifier."""
        runway = {'le_ident': '12', 'he_ident': '30'}
        assert get_runway_end_data(runway, '09') is None


class TestGeometry:
    """Test geometric calculations."""

    def test_nm_to_deg_lat(self):
        """Test nautical miles to degrees latitude conversion."""
        # 60 nm = 1 degree latitude
        assert abs(nm_to_deg_lat(60) - 1.0) < 0.001
        assert abs(nm_to_deg_lat(30) - 0.5) < 0.001

    def test_point_at_distance_bearing_north(self):
        """Test point calculation going north."""
        lat, lon = point_at_distance_bearing(37.0, -122.0, 1.0, 0)  # 1nm north
        assert lat > 37.0  # Should be north
        assert abs(lon - (-122.0)) < 0.001  # Longitude unchanged

    def test_point_at_distance_bearing_east(self):
        """Test point calculation going east."""
        lat, lon = point_at_distance_bearing(37.0, -122.0, 1.0, 90)  # 1nm east
        assert abs(lat - 37.0) < 0.001  # Latitude unchanged
        assert lon > -122.0  # Should be east (less negative)

    def test_format_heading_range_normal(self):
        """Test heading range without wraparound."""
        start, end = format_heading_range(90, 20)
        assert start == 70
        assert end == 110

    def test_format_heading_range_wraparound(self):
        """Test heading range with wraparound at 360."""
        start, end = format_heading_range(350, 20)
        assert start == 330
        assert end == 10  # Wraps around

        start, end = format_heading_range(10, 20)
        assert start == 350  # Wraps around
        assert end == 30

    def test_generate_circle_polygon(self):
        """Test circle polygon generation."""
        poly = generate_circle_polygon(37.0, -122.0, 1.0, num_points=8)
        # Should have 9 points (8 + closing point)
        assert len(poly) == 9
        # First and last should be same (closed polygon)
        assert poly[0] == poly[-1]
        # All points should be roughly 1nm from center
        for lat, lon in poly[:-1]:
            # Very rough check - points should be within reasonable range
            assert abs(lat - 37.0) < 0.02
            assert abs(lon - (-122.0)) < 0.03

    def test_generate_wedge_polygon(self):
        """Test wedge polygon generation."""
        poly = generate_wedge_polygon(37.0, -122.0, 90, 3.0, 0.1, 0.5)
        # Should have 5 points (4 corners + closing)
        assert len(poly) == 5
        # First and last should be same (closed polygon)
        assert poly[0] == poly[-1]


class TestEndToEnd:
    """End-to-end integration tests."""

    def test_full_generation_ksql(self):
        """Test full config generation for KSQL."""
        # Load real data
        airport = load_airport("KSQL")
        assert airport is not None

        runways = load_runways("KSQL")
        assert len(runways) >= 1

        longest = get_longest_runway(runways)
        assert longest is not None

        lower_end = get_lower_runway_end(longest)
        end_data = get_runway_end_data(longest, lower_end)
        assert end_data is not None

        # Verify reasonable values
        assert 100 < end_data['heading'] < 180  # Runway 12 heading ~138
        assert 37.0 < end_data['lat'] < 38.0
        assert -123.0 < end_data['lon'] < -122.0
