"""
Test that the hello_world example works as expected.
"""

import subprocess
import sys

def test_simple_monitor_directory():
    """Run simple_monitor with directory input and verify it processes data."""
    result = subprocess.run(
        [
            sys.executable,
            "src/analyzers/simple_monitor.py",
            "--directory", "tests/sample_readsb_data",
            "examples/hello_world_rules.yaml"
        ],
        capture_output=True,
        text=True,
        timeout=60,
        check=False
    )

    # Check that it ran without error
    assert result.returncode == 0, f"simple_monitor failed: {result.stderr}"

    # Check that it processed some data (should see "Reading data" and "Processing" messages)
    assert "Reading data" in result.stdout, f"Expected 'Reading data' in output: {result.stdout}"
    assert "Processing" in result.stdout, f"Expected 'Processing' in output: {result.stdout}"

    # Verify data was parsed and processed
    assert "First point seen at" in result.stdout, \
        f"Expected 'First point seen at' in output: {result.stdout}"
    assert "parsed 8022 points" in result.stderr, \
        f"Expected 'parsed 8022 points' in output: {result.stderr}"

    # Verify specific aircraft was printed (from hello_world_rules.yaml print action)
    assert "N237AK" in result.stdout, \
        f"Expected aircraft 'N237AK' in output: {result.stdout}"
