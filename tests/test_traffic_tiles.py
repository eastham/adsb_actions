"""Tests for traffic tile generation."""

import math
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "tools"))
from traffic_tiles import (
    latlon_to_tile_pixel,
    tile_to_latlon_bounds,
    tile_center,
    altitude_to_band,
    band_to_color,
    NUM_BANDS,
)


class TestLatLonToTilePixel:
    """Test Web Mercator tile coordinate conversion."""

    def test_origin(self):
        """0,0 at zoom 11 should be tile (1024, 1024), pixel near center."""
        tx, ty, px, py = latlon_to_tile_pixel(0.0, 0.0, 11)
        assert tx == 1024
        assert ty == 1024
        # Pixel should be at (0, 0) of that tile since 0,0 is exact tile boundary
        assert 0 <= px <= 255
        assert 0 <= py <= 255

    def test_north_america(self):
        """San Francisco area (~37.7, -122.4) should give reasonable tile coords."""
        tx, ty, px, py = latlon_to_tile_pixel(37.7749, -122.4194, 11)
        # At zoom 11, SF is approximately tile (327, 791)
        assert 320 <= tx <= 335
        assert 785 <= ty <= 800
        assert 0 <= px <= 255
        assert 0 <= py <= 255

    def test_pixel_range(self):
        """Pixels should always be in 0-255 range."""
        for lat in [-60, -30, 0, 30, 60]:
            for lon in [-180, -90, 0, 90, 179.9]:
                _, _, px, py = latlon_to_tile_pixel(lat, lon, 11)
                assert 0 <= px <= 255, f"px={px} for lat={lat}, lon={lon}"
                assert 0 <= py <= 255, f"py={py} for lat={lat}, lon={lon}"


class TestTileToLatLonBounds:
    """Test tile bounds conversion."""

    def test_roundtrip(self):
        """Converting a point to tile then back to bounds should contain the point."""
        lat, lon = 37.7749, -122.4194
        tx, ty, _, _ = latlon_to_tile_pixel(lat, lon, 11)
        bounds = tile_to_latlon_bounds(tx, ty, 11)
        sw_lat, sw_lon = bounds[0]
        ne_lat, ne_lon = bounds[1]
        assert sw_lat <= lat <= ne_lat
        assert sw_lon <= lon <= ne_lon

    def test_tile_size(self):
        """Tile at zoom 11 near equator should cover roughly 0.17 degrees."""
        # Use tile near equator (y=1024) for predictable size
        bounds = tile_to_latlon_bounds(500, 1024, 11)
        lat_span = bounds[1][0] - bounds[0][0]
        lon_span = bounds[1][1] - bounds[0][1]
        assert 0.1 < lat_span < 0.3
        assert 0.1 < lon_span < 0.2

    def test_sw_ne_ordering(self):
        """SW corner should be less than NE corner."""
        bounds = tile_to_latlon_bounds(327, 791, 11)
        assert bounds[0][0] < bounds[1][0]  # sw_lat < ne_lat
        assert bounds[0][1] < bounds[1][1]  # sw_lon < ne_lon


class TestTileCenter:
    def test_center_within_bounds(self):
        bounds = tile_to_latlon_bounds(327, 791, 11)
        lat, lon = tile_center(327, 791, 11)
        assert bounds[0][0] < lat < bounds[1][0]
        assert bounds[0][1] < lon < bounds[1][1]


class TestAltitudeToBand:
    def test_floor(self):
        """Alt at floor should map to band 0."""
        assert altitude_to_band(0, 0, 4000) == 0

    def test_ceiling(self):
        """Alt at ceiling should map to max band."""
        assert altitude_to_band(4000, 0, 4000) == NUM_BANDS - 1

    def test_midpoint(self):
        """Midpoint altitude should map to middle band."""
        band = altitude_to_band(2000, 0, 4000)
        assert NUM_BANDS // 2 - 1 <= band <= NUM_BANDS // 2 + 1

    def test_below_floor_clamps(self):
        """Below floor should clamp to band 0."""
        assert altitude_to_band(-500, 0, 4000) == 0

    def test_above_ceiling_clamps(self):
        """Above ceiling should clamp to max band."""
        assert altitude_to_band(5000, 0, 4000) == NUM_BANDS - 1


class TestBandToColor:
    def test_band_zero_is_purple(self):
        r, g, b = band_to_color(0)
        assert r == 128.0
        assert g == 0.0
        assert b == 255.0

    def test_band_max_is_light_blue(self):
        r, g, b = band_to_color(NUM_BANDS - 1)
        assert r == 150.0
        assert g == 220.0
        assert b == 255.0

    def test_midpoint_is_blue(self):
        mid = NUM_BANDS // 2
        r, g, b = band_to_color(mid)
        # Midpoint should be in the blue/light-blue range
        assert r < 120
        assert g > 50
        assert b == 255.0
