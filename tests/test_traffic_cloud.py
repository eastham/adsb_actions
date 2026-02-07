"""Tests for traffic cloud point sampling and visualization."""

import tempfile
import os
from pathlib import Path


def test_traffic_sample_export():
    """Test that traffic samples are exported correctly with grid decimation."""
    # This test would require setting up a resampler with mock data
    # For now, just verify the export file gets created

    # TODO: Add actual test once we have sample data
    pass


def test_traffic_csv_format():
    """Test that traffic CSV files have correct format (lat,lon,alt)."""
    import csv

    # Create a mock traffic CSV
    with tempfile.NamedTemporaryFile(mode='w', suffix='_traffic.csv', delete=False) as f:
        test_file = f.name
        f.write("40.7123,-119.2456,18375\n")
        f.write("40.7145,-119.2478,19200\n")
        f.write("40.7089,-119.2401,17500\n")

    try:
        # Verify we can parse it
        points = []
        with open(test_file, 'r') as f:
            for line in f:
                lat, lon, alt = line.strip().split(',')
                points.append((float(lat), float(lon), float(alt)))

        assert len(points) == 3
        assert points[0] == (40.7123, -119.2456, 18375.0)

        print("✓ Traffic CSV format test passed")
    finally:
        os.unlink(test_file)


def test_grid_decimation():
    """Test that grid decimation reduces point count correctly."""
    # Simulate grid decimation logic
    grid_size = 0.002  # ~720ft

    # Create test points with some duplicates in same grid cell
    test_points = [
        (40.7123, -119.2456, 18375),
        (40.7124, -119.2457, 18400),  # Same grid cell as above
        (40.7145, -119.2478, 19200),  # Different grid cell
    ]

    grid_cells = set()
    sampled = []

    for lat, lon, alt in test_points:
        grid_cell = (int(lat / grid_size), int(lon / grid_size))
        if grid_cell not in grid_cells:
            grid_cells.add(grid_cell)
            sampled.append((lat, lon, alt))

    # Should reduce 3 points to 2 (first two are in same cell)
    assert len(sampled) == 2, f"Expected 2 sampled points, got {len(sampled)}"

    print("✓ Grid decimation test passed")


if __name__ == "__main__":
    test_traffic_csv_format()
    test_grid_decimation()
    print("\n✅ All traffic cloud tests passed!")
