"""Tests for streaming functionality and resampler bbox filtering."""

import gzip
import logging
import os
import tempfile

import yaml

import testinfra
from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions
from adsb_actions.resampler import Resampler
from adsb_actions.location import Location
from lib import replay


def test_yield_from_sorted_file():
    """Test that yield_from_sorted_file correctly streams from a sorted JSONL file."""

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    # Create a temporary sorted JSONL file with larger time gaps to trigger dummy entries
    # (dummy entries inserted every 20 iterations for gaps)
    test_points = [
        '{"now": 1000, "alt_baro": 4000, "lat": 40.0, "lon": -119.0, "hex": "a061d9", "flight": "N12345"}\n',
        '{"now": 1001, "alt_baro": 4100, "lat": 40.1, "lon": -119.1, "hex": "a061d9", "flight": "N12345"}\n',
        '{"now": 1005, "alt_baro": 4200, "lat": 40.2, "lon": -119.2, "hex": "a061d9", "flight": "N12345"}\n',
        '{"now": 1010, "alt_baro": 4300, "lat": 40.3, "lon": -119.3, "hex": "b12345", "flight": "N67890"}\n',
    ]

    with tempfile.NamedTemporaryFile(mode='wb', suffix='.jsonl.gz', delete=False) as f:
        temp_path = f.name
        with gzip.open(f, 'wt') as gz:
            for point in test_points:
                gz.write(point)

    try:
        # Test streaming without dummy entries
        points = list(replay.yield_from_sorted_file(temp_path, insert_dummy_entries=False))
        assert len(points) == 4, f"Expected 4 points, got {len(points)}"
        assert points[0]['now'] == 1000
        assert points[1]['now'] == 1001
        assert points[2]['now'] == 1005
        assert points[3]['now'] == 1010

    finally:
        os.unlink(temp_path)


def test_yield_from_sorted_file_plain_json():
    """Test that yield_from_sorted_file works with plain (non-gzipped) JSON files."""

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    # Use the existing test file which is plain JSONL
    points = list(replay.yield_from_sorted_file("tests/1hr.json", insert_dummy_entries=False))

    # Should have many points from the 1-hour test file
    assert len(points) > 1000, f"Expected >1000 points from 1hr.json, got {len(points)}"

    # First point should have expected fields
    assert 'now' in points[0]
    assert 'lat' in points[0]
    assert 'lon' in points[0]

    # Verify timestamps are sorted
    timestamps = [p.get('now', 0) for p in points if p.get('flight') != 'N/A']
    for i in range(1, len(timestamps)):
        assert timestamps[i] >= timestamps[i-1], \
            f"Timestamps not sorted at index {i}: {timestamps[i-1]} > {timestamps[i]}"


def test_yield_from_sorted_file_with_large_gap():
    """Test that yield_from_sorted_file inserts dummy entries for large time gaps."""

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    # Create data with a 50-second gap to ensure dummy entries are inserted
    # (dummy entries inserted every 20 iterations)
    test_points = [
        '{"now": 1000, "alt_baro": 4000, "lat": 40.0, "lon": -119.0, "hex": "a061d9", "flight": "N12345"}\n',
        '{"now": 1050, "alt_baro": 4100, "lat": 40.1, "lon": -119.1, "hex": "a061d9", "flight": "N12345"}\n',
    ]

    with tempfile.NamedTemporaryFile(mode='wb', suffix='.jsonl.gz', delete=False) as f:
        temp_path = f.name
        with gzip.open(f, 'wt') as gz:
            for point in test_points:
                gz.write(point)

    try:
        # Test streaming with dummy entries (fills gaps)
        points_with_dummies = list(replay.yield_from_sorted_file(temp_path, insert_dummy_entries=True))
        # Should have original 2 points plus some dummy entries for the 50-second gap
        # Gap is 49 seconds (1001-1049), dummy inserted every 20 iterations = ~2 dummies
        assert len(points_with_dummies) > 2, f"Expected more than 2 points with dummies, got {len(points_with_dummies)}"

        # Verify dummy entries have 'flight': 'N/A'
        dummy_count = sum(1 for p in points_with_dummies if p.get('flight') == 'N/A')
        assert dummy_count > 0, f"Expected at least one dummy entry, got {dummy_count}"

    finally:
        os.unlink(temp_path)


def test_yield_from_sorted_file_integration():
    """Test that yield_from_sorted_file works correctly with AdsbActions.loop()."""

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    YAML_STRING = """
      config:
        kmls:
          - tests/test1.kml
      rules: {}
    """

    # Create test data - multiple aircraft, sorted by time
    test_points = [
        '{"now": 1000, "alt_baro": 4000, "gscp": 128, "lat": 40.0, "lon": -119.0, "track": 90, "hex": "a061d9", "flight": "N12345"}\n',
        '{"now": 1001, "alt_baro": 4100, "gscp": 128, "lat": 40.01, "lon": -119.01, "track": 90, "hex": "a061d9", "flight": "N12345"}\n',
        '{"now": 1002, "alt_baro": 4200, "gscp": 128, "lat": 37.0, "lon": -122.0, "track": 180, "hex": "b12345", "flight": "N67890"}\n',
        '{"now": 1003, "alt_baro": 4300, "gscp": 128, "lat": 40.02, "lon": -119.02, "track": 90, "hex": "a061d9", "flight": "N12345"}\n',
    ]

    with tempfile.NamedTemporaryFile(mode='wb', suffix='.jsonl.gz', delete=False) as f:
        temp_path = f.name
        with gzip.open(f, 'wt') as gz:
            for point in test_points:
                gz.write(point)

    try:
        Stats.reset()
        yaml_data = yaml.safe_load(YAML_STRING)
        # Use resample=False to test streaming without bbox filtering complications
        adsb_actions = AdsbActions(yaml_data, resample=False)
        testinfra.setup_test_callback(adsb_actions)

        # Stream from sorted file
        iterator = replay.yield_from_sorted_file(temp_path, insert_dummy_entries=False)
        adsb_actions.loop(iterator_data=iterator)

        # Verify flights were tracked (flight_dict is the actual attribute)
        assert len(adsb_actions.flights.flight_dict) == 2, \
            f"Expected 2 flights, got {len(adsb_actions.flights.flight_dict)}"

    finally:
        os.unlink(temp_path)


def test_resampler_bbox_filtering():
    """Test that resampler filters out points not in any bbox."""

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    # Create a simple bbox that only covers a small area
    # Using the bbox structure from the codebase (dataclass with specific fields)
    from adsb_actions.bboxes import Bboxes, Bbox

    # Create a bbox around lat=40.0, lon=-119.0 (1 degree square)
    # Bbox fields: polygon_coords, minalt, maxalt, starthdg, endhdg, name
    bbox = Bbox(
        polygon_coords=[
            (-119.5, 39.5),  # SW corner
            (-118.5, 39.5),  # SE corner
            (-118.5, 40.5),  # NE corner
            (-119.5, 40.5),  # NW corner
            (-119.5, 39.5),  # Close polygon
        ],
        minalt=0,
        maxalt=50000,
        starthdg=0,
        endhdg=360,
        name="test_bbox"
    )

    # Create a Bboxes container manually (avoiding KML parsing)
    class MockBboxes:
        def __init__(self):
            self.boxes = []

    bboxes = MockBboxes()
    bboxes.boxes = [bbox]

    # Create resampler with bbox filtering
    resampler = Resampler(bboxes=[bboxes])

    # Location inside bbox
    loc_inside = Location(
        lat=40.0, lon=-119.0, alt_baro=5000, now=1000,
        flight="N12345", hex="a061d9", tail="N12345", gs=100, track=90
    )

    # Location outside bbox (different longitude)
    loc_outside = Location(
        lat=40.0, lon=-122.0, alt_baro=5000, now=1001,
        flight="N67890", hex="b12345", tail="N67890", gs=100, track=90
    )

    resampler.add_location(loc_inside)
    resampler.add_location(loc_outside)

    # Only the inside location should be stored
    assert len(resampler.locations_by_flight_id) == 1, \
        f"Expected 1 flight tracked, got {len(resampler.locations_by_flight_id)}"
    assert "N12345_1" in resampler.locations_by_flight_id, \
        "Expected N12345_1 to be tracked"
    assert resampler.filtered_ctr == 1, \
        f"Expected 1 filtered point, got {resampler.filtered_ctr}"


def test_resampler_no_bbox_filtering():
    """Test that resampler stores all points when no bboxes configured."""

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    # Create resampler without bbox filtering
    resampler = Resampler(bboxes=None)

    # Two locations at different places
    loc1 = Location(
        lat=40.0, lon=-119.0, alt_baro=5000, now=1000,
        flight="N12345", hex="a061d9", tail="N12345", gs=100, track=90
    )
    loc2 = Location(
        lat=37.0, lon=-122.0, alt_baro=5000, now=1001,
        flight="N67890", hex="b12345", tail="N67890", gs=100, track=90
    )

    resampler.add_location(loc1)
    resampler.add_location(loc2)

    # Both locations should be stored
    assert len(resampler.locations_by_flight_id) == 2, \
        f"Expected 2 flights tracked, got {len(resampler.locations_by_flight_id)}"
    assert resampler.filtered_ctr == 0, \
        f"Expected 0 filtered points, got {resampler.filtered_ctr}"


def test_resampler_bbox_filtering_with_interpolation():
    """Test that bbox filtering works correctly with interpolation."""

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    from adsb_actions.bboxes import Bbox

    # Create a bbox around lat=40.0, lon=-119.0
    bbox = Bbox(
        polygon_coords=[
            (-119.5, 39.5),
            (-118.5, 39.5),
            (-118.5, 40.5),
            (-119.5, 40.5),
            (-119.5, 39.5),
        ],
        minalt=0,
        maxalt=50000,
        starthdg=0,
        endhdg=360,
        name="test_bbox"
    )

    # Create a mock Bboxes container
    class MockBboxes:
        def __init__(self):
            self.boxes = []

    bboxes = MockBboxes()
    bboxes.boxes = [bbox]

    # Create resampler with bbox filtering
    resampler = Resampler(bboxes=[bboxes])

    # Two points inside bbox, with a 5-second gap (should interpolate)
    loc1 = Location(
        lat=40.0, lon=-119.0, alt_baro=5000, now=1000,
        flight="N12345", hex="a061d9", tail="N12345", gs=100, track=90
    )
    loc2 = Location(
        lat=40.01, lon=-119.01, alt_baro=5100, now=1005,
        flight="N12345", hex="a061d9", tail="N12345", gs=100, track=90
    )

    resampler.add_location(loc1)
    resampler.add_location(loc2)

    # Should have 6 timestamps (1000, 1001, 1002, 1003, 1004, 1005)
    assert len(resampler.locations_by_time) == 6, \
        f"Expected 6 timestamps, got {len(resampler.locations_by_time)}"

    # Verify interpolated point exists
    assert 1003 in resampler.locations_by_time, "Missing interpolated timestamp 1003"
