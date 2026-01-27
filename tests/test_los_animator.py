"""Tests for the LOS animator module."""

import os
import tempfile

from adsb_actions.resampler import Resampler
from adsb_actions.location import Location
from postprocessing.los_animator import LOSAnimator


class TestLOSAnimator:
    """Test the LOSAnimator class."""

    def _create_test_resampler(self):
        """Create a resampler with test flight data in locations_by_time."""
        resampler = Resampler()

        # Create two flights with overlapping time windows
        base_time = 1700000000  # Some arbitrary timestamp

        # Flight 1: N12345_1 - moving east
        for i in range(180):  # 3 minutes of data
            loc = Location(
                lat=37.0 + i * 0.0001,  # Moving slightly north
                lon=-122.0 + i * 0.001,  # Moving east
                alt_baro=2000,
                now=base_time + i,
                flight="N12345_1",
                hex="abc123",
                tail="N12345",
                gs=120,
                track=90
            )
            t = int(loc.now)
            if t not in resampler.locations_by_time:
                resampler.locations_by_time[t] = []
            resampler.locations_by_time[t].append(loc)

        # Flight 2: N67890_1 - moving west, converging
        for i in range(180):
            loc = Location(
                lat=37.0 + i * 0.0001,
                lon=-121.8 - i * 0.001,  # Moving west
                alt_baro=2100,
                now=base_time + i,
                flight="N67890_1",
                hex="def456",
                tail="N67890",
                gs=110,
                track=270
            )
            t = int(loc.now)
            if t not in resampler.locations_by_time:
                resampler.locations_by_time[t] = []
            resampler.locations_by_time[t].append(loc)

        return resampler, base_time

    def test_find_flight_id_exact_match(self):
        """Test that exact flight_id match works."""
        resampler, base_time = self._create_test_resampler()
        animator = LOSAnimator(resampler)

        # Should find exact match (searches locations_by_time)
        fid = animator._find_flight_id("N12345_1", base_time + 60)
        assert fid == "N12345_1"

    def test_find_flight_id_tail_lookup(self):
        """Test that tail number lookup finds suffixed flight_id."""
        resampler, base_time = self._create_test_resampler()
        animator = LOSAnimator(resampler)

        # Should find N12345_1 from just N12345
        fid = animator._find_flight_id("N12345", base_time + 60)
        assert fid == "N12345_1"

    def test_find_flight_id_not_found(self):
        """Test that missing tail returns None."""
        resampler, base_time = self._create_test_resampler()
        animator = LOSAnimator(resampler)

        fid = animator._find_flight_id("NXXXXX", base_time + 60)
        assert fid is None

    def test_get_positions_in_window(self):
        """Test extracting positions within a time window."""
        resampler, base_time = self._create_test_resampler()
        animator = LOSAnimator(resampler)

        # Get 60 seconds of positions
        positions = animator._get_positions_in_window(
            "N12345_1",
            base_time + 30,
            base_time + 90
        )

        assert len(positions) == 61  # inclusive range
        assert positions[0].now == base_time + 30
        assert positions[-1].now == base_time + 90

    def test_build_features(self):
        """Test GeoJSON feature building."""
        resampler, base_time = self._create_test_resampler()
        animator = LOSAnimator(resampler)

        positions = animator._get_positions_in_window(
            "N12345_1", base_time, base_time + 10
        )

        features = animator._build_features(positions, "blue")

        assert len(features) == 11
        assert features[0]["type"] == "Feature"
        assert features[0]["geometry"]["type"] == "Point"
        assert "time" in features[0]["properties"]
        assert features[0]["properties"]["iconstyle"]["color"] == "blue"

    def test_build_trail_features(self):
        """Test trail (LineString) feature building."""
        resampler, base_time = self._create_test_resampler()
        animator = LOSAnimator(resampler)

        positions = animator._get_positions_in_window(
            "N12345_1", base_time, base_time + 5
        )

        trails = animator._build_trail_features(positions, "blue")

        # Should have n-1 trail segments for n positions
        assert len(trails) == 5
        assert trails[0]["geometry"]["type"] == "LineString"
        # First trail has 2 points, last has all 6
        assert len(trails[0]["geometry"]["coordinates"]) == 2
        assert len(trails[-1]["geometry"]["coordinates"]) == 6

    def test_animate_los_creates_file(self):
        """Test that animate_los creates an HTML file."""
        resampler, base_time = self._create_test_resampler()
        animator = LOSAnimator(resampler)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = os.path.join(tmpdir, "test_animation.html")

            result = animator.animate_los(
                "N12345_1",
                "N67890_1",
                base_time + 90,  # Event at middle of window
                window_before=60,
                window_after=30,
                output_file=output_file
            )

            assert result == output_file
            assert os.path.exists(output_file)

            # Check file has content
            with open(output_file, 'r') as f:
                content = f.read()
                assert "LOS Event" in content
                assert "N12345" in content
                assert "N67890" in content
                # TimestampedGeoJson generates leaflet-based animation
                assert "leaflet" in content.lower()

    def test_animate_los_missing_flight(self):
        """Test that missing flight data returns None."""
        resampler, base_time = self._create_test_resampler()
        animator = LOSAnimator(resampler)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = os.path.join(tmpdir, "test.html")

            result = animator.animate_los(
                "N12345_1",
                "MISSING_1",  # Not in resampler
                base_time + 90,
                output_file=output_file
            )

            assert result is None
            assert not os.path.exists(output_file)


class MockFlight:
    """Mock flight object for testing."""
    def __init__(self, flight_id):
        self.flight_id = flight_id


class MockLOS:
    """Mock LOS object for testing."""
    def __init__(self, tail1, tail2, create_time):
        self.flight1 = MockFlight(tail1)
        self.flight2 = MockFlight(tail2)
        self.create_time = create_time


class TestAnimateFromLOS:
    """Test animate_from_los_object method."""

    def test_animate_from_los_object(self):
        """Test creating animation from LOS object."""
        # Create resampler with test data in locations_by_time
        resampler = Resampler()
        base_time = 1700000000

        # Add flight data to locations_by_time
        for suffix, tail in [("1", "N111"), ("1", "N222")]:
            for i in range(180):
                loc = Location(
                    lat=37.0 + i * 0.0001,
                    lon=-122.0 + (1 if tail == "N111" else -1) * i * 0.001,
                    alt_baro=2000,
                    now=base_time + i,
                    flight=f"{tail}_{suffix}",
                    hex="abc",
                    tail=tail,
                    gs=100,
                    track=90
                )
                t = int(loc.now)
                if t not in resampler.locations_by_time:
                    resampler.locations_by_time[t] = []
                resampler.locations_by_time[t].append(loc)

        animator = LOSAnimator(resampler)

        # Create mock LOS (note: no suffix in flight_id, like real LOS objects)
        los = MockLOS("N111", "N222", base_time + 90)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = os.path.join(tmpdir, "test_los.html")

            result = animator.animate_from_los_object(
                los,
                output_file=output_file
            )

            assert result == output_file
            assert os.path.exists(output_file)
