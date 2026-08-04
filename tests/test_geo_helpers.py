"""Tests for geographic calculation helpers."""

import pytest
from adsb_actions.geo_helpers import nm_to_lat_lon_offsets


class TestNmToLatLonOffsets:
    """Test nautical miles to lat/lon degree offset conversion."""

    def test_at_equator(self):
        """Test conversion at the equator (no longitude compression)."""
        lat_off, lon_off = nm_to_lat_lon_offsets(60.0, 0.0)

        # At equator: 1 degree ≈ 60 nm for both lat and lon
        assert abs(lat_off - 1.0) < 0.001
        assert abs(lon_off - 1.0) < 0.001

    def test_at_45_degrees_north(self):
        """Test conversion at 45°N (longitude compression by cos(45°) ≈ 0.707)."""
        lat_off, lon_off = nm_to_lat_lon_offsets(60.0, 45.0)

        # Latitude offset unchanged
        assert abs(lat_off - 1.0) < 0.001

        # Longitude offset larger due to compression: ~1.414 degrees
        # cos(45°) ≈ 0.707, so lon_offset = 60 / (60 * 0.707) ≈ 1.414
        assert abs(lon_off - 1.414) < 0.01

    def test_at_high_latitude(self):
        """Test conversion at 60°N (longitude compression by cos(60°) = 0.5)."""
        lat_off, lon_off = nm_to_lat_lon_offsets(60.0, 60.0)

        # Latitude offset unchanged
        assert abs(lat_off - 1.0) < 0.001

        # Longitude offset doubled: 2.0 degrees
        # cos(60°) = 0.5, so lon_offset = 60 / (60 * 0.5) = 2.0
        assert abs(lon_off - 2.0) < 0.001

    def test_small_radius(self):
        """Test with a small radius (5nm)."""
        lat_off, lon_off = nm_to_lat_lon_offsets(5.0, 0.0)

        # 5nm ≈ 0.0833 degrees at equator
        expected_offset = 5.0 / 60.0
        assert abs(lat_off - expected_offset) < 0.0001
        assert abs(lon_off - expected_offset) < 0.0001

    def test_large_radius(self):
        """Test with a large radius (600nm = 10 degrees)."""
        lat_off, lon_off = nm_to_lat_lon_offsets(600.0, 0.0)

        # 600nm = 10 degrees
        assert abs(lat_off - 10.0) < 0.001
        assert abs(lon_off - 10.0) < 0.001

    def test_southern_hemisphere(self):
        """Test in southern hemisphere (cos is same, result should be identical)."""
        lat_off_north, lon_off_north = nm_to_lat_lon_offsets(60.0, 45.0)
        lat_off_south, lon_off_south = nm_to_lat_lon_offsets(60.0, -45.0)

        # cos(45°) = cos(-45°), so results should be identical
        assert abs(lat_off_north - lat_off_south) < 0.0001
        assert abs(lon_off_north - lon_off_south) < 0.0001

    def test_realistic_adsb_scenario(self):
        """Test with realistic ADS-B monitoring scenario."""
        # 20nm radius around Reno-Tahoe airport (KRNO: ~39.5°N)
        lat_off, lon_off = nm_to_lat_lon_offsets(20.0, 39.5)

        # Latitude: 20nm ≈ 0.333 degrees
        assert abs(lat_off - 0.333) < 0.001

        # Longitude: at 39.5°N, cos ≈ 0.773, so 20/(60*0.773) ≈ 0.431 degrees
        assert abs(lon_off - 0.431) < 0.01
