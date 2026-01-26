"""Test print_csv action outputs CSV lines compatible with visualizer.py"""

import logging
import pytest
import yaml
from io import StringIO

import testinfra
from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions

YAML_STRING = """
  rules:
    csv_test:
      conditions:
        min_alt: 4000
      actions:
        print_csv: "test_event"
"""

JSON_STRING = '{"now": 1661692178, "alt_baro": 5000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'
JSON_STRING_LOW = '{"now": 1661692178, "alt_baro": 3000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N67890"}'

@pytest.fixture
def adsb_state():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    Stats.reset()
    yaml_data = yaml.safe_load(YAML_STRING)
    adsb_actions = AdsbActions(yaml_data)
    yield adsb_actions

def test_print_csv_action(adsb_state, caplog):
    """Test that print_csv action outputs CSV lines with correct format."""
    caplog.clear()  # Clear any prior log records for test isolation
    with caplog.at_level(logging.INFO, logger="adsb_actions.rules"):
        adsb_state.loop(JSON_STRING)

    # Check that CSV output was logged
    csv_lines = [r.message for r in caplog.records if "CSV OUTPUT FOR POSTPROCESSING" in r.message]
    assert len(csv_lines) == 1, f"Expected 1 CSV line, got {len(csv_lines)}. Records: {[r.message for r in caplog.records]}"

    csv_line = csv_lines[0]
    # Verify key fields are present
    assert "1661692178" in csv_line  # timestamp
    assert "40.763537" in csv_line   # lat
    assert "-119.2122323" in csv_line  # lon
    assert "5000" in csv_line        # alt
    assert "N12345" in csv_line      # tail
    assert "test_event" in csv_line  # event type
    assert "globe.adsbexchange.com" in csv_line  # replay link

def test_print_csv_no_match(adsb_state, caplog):
    """Test that print_csv doesn't fire when conditions don't match."""
    caplog.clear()  # Clear any prior log records for test isolation
    with caplog.at_level(logging.INFO, logger="adsb_actions.rules"):
        adsb_state.loop(JSON_STRING_LOW)  # Below min_alt threshold

    csv_lines = [r.message for r in caplog.records if "CSV OUTPUT FOR POSTPROCESSING" in r.message]
    assert len(csv_lines) == 0
