"""
Test the additional conditions (squawk, emergency, category, baro_rate, callsign_prefix, on_ground)
using the sample_readsb_data which contains these fields.

These tests verify that conditions are actually filtering, not just matching everything.
"""

import logging
import yaml
import testinfra

from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions
from tools.analysis import replay

# Counters for callbacks - using dicts to track matches vs non-matches
counters = {}

def make_callback(name):
    def cb(flight):
        counters[name] = counters.get(name, 0) + 1
    return cb

def run_with_yaml(yaml_string):
    """Helper to run the sample data through a YAML config and return match count."""
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.WARNING)
    testinfra.set_all_loggers(logging.WARNING)
    Stats.reset()

    yaml_data = yaml.safe_load(yaml_string)
    allpoints = replay.read_data('tests/sample_readsb_data')
    allpoints_iter = replay.yield_json_data(allpoints, insert_dummy_entries=False)

    adsb_actions = AdsbActions(yaml_data)
    adsb_actions.register_callback("test_cb", make_callback("test"))
    adsb_actions.loop(iterator_data=allpoints_iter)

    return counters.get("test", 0)


def test_squawk_filtering():
    """Test that squawk condition filters correctly - matching vs non-matching codes."""
    global counters

    # Test with codes that exist in sample data
    counters = {}
    matches_common = run_with_yaml("""
rules:
  test:
    conditions:
      squawk: [1200, 1000, 4523, 5765, 4514, 4517]
    actions:
      callback: test_cb
""")

    # Test with codes that shouldn't exist
    counters = {}
    matches_rare = run_with_yaml("""
rules:
  test:
    conditions:
      squawk: [9999, 8888, 7777]
    actions:
      callback: test_cb
""")

    assert matches_common > 0, "Common squawk codes should match"
    assert matches_rare == 0, "Non-existent squawk codes should not match"
    assert matches_common > matches_rare, "Common codes should match more than rare codes"


def test_emergency_filtering():
    """Test that emergency condition filters - 'none' vs 'any' vs specific."""
    global counters

    # Most aircraft should have emergency=none
    counters = {}
    matches_none = run_with_yaml("""
rules:
  test:
    conditions:
      emergency: none
    actions:
      callback: test_cb
""")

    # Very few (likely zero) should have actual emergencies in sample data
    counters = {}
    matches_any = run_with_yaml("""
rules:
  test:
    conditions:
      emergency: any
    actions:
      callback: test_cb
""")

    assert matches_none > 0, "emergency=none should match normal aircraft"
    assert matches_none > matches_any, "More aircraft should have no emergency than have emergencies"


def test_category_filtering():
    """Test that category condition filters by aircraft type."""
    global counters

    # Light/small aircraft (A1, A2) - common in GA
    counters = {}
    matches_light = run_with_yaml("""
rules:
  test:
    conditions:
      category: [A1, A2]
    actions:
      callback: test_cb
""")

    # Surface vehicles (C1, C2, C3) - very rare in flight data
    counters = {}
    matches_surface = run_with_yaml("""
rules:
  test:
    conditions:
      category: [C1, C2, C3]
    actions:
      callback: test_cb
""")

    assert matches_light > 0, "Light aircraft categories should match"
    assert matches_light > matches_surface, "More aircraft than surface vehicles expected"


def test_vertical_rate_filtering():
    """Test that vertical rate conditions filter climbing vs descending."""
    global counters

    # Climbing aircraft (positive baro_rate)
    counters = {}
    matches_climbing = run_with_yaml("""
rules:
  test:
    conditions:
      min_vertical_rate: 500
    actions:
      callback: test_cb
""")

    # Descending aircraft (negative baro_rate)
    counters = {}
    matches_descending = run_with_yaml("""
rules:
  test:
    conditions:
      max_vertical_rate: -500
    actions:
      callback: test_cb
""")

    # Extreme climb rate (less common)
    counters = {}
    matches_extreme_climb = run_with_yaml("""
rules:
  test:
    conditions:
      min_vertical_rate: 2000
    actions:
      callback: test_cb
""")

    assert matches_climbing > 0, "Should find climbing aircraft"
    assert matches_descending > 0, "Should find descending aircraft"
    # Extreme climb rates should be less common than moderate climb rates
    assert matches_climbing > matches_extreme_climb, \
        "Moderate climbs should be more common than extreme climbs"


def test_callsign_prefix_filtering():
    """Test that callsign prefix filters correctly."""
    global counters

    # N-numbers (US registration) - common
    counters = {}
    matches_n = run_with_yaml("""
rules:
  test:
    conditions:
      callsign_prefix: N
    actions:
      callback: test_cb
""")

    # Specific airline that's unlikely in sample data
    counters = {}
    matches_unlikely = run_with_yaml("""
rules:
  test:
    conditions:
      callsign_prefix: [QFA, SIA, BAW]
    actions:
      callback: test_cb
""")

    assert matches_n > 0, "N-prefix callsigns should match"
    assert matches_n > matches_unlikely, "US registrations should be more common than foreign airlines"


def test_combined_conditions_filter():
    """Test that combining conditions reduces matches (AND logic)."""
    global counters

    # Single condition - should match many
    counters = {}
    matches_single = run_with_yaml("""
rules:
  test:
    conditions:
      callsign_prefix: N
    actions:
      callback: test_cb
""")

    # Two conditions - should match fewer
    counters = {}
    matches_two = run_with_yaml("""
rules:
  test:
    conditions:
      callsign_prefix: N
      category: [A1, A2]
    actions:
      callback: test_cb
""")

    # Three conditions - should match even fewer
    counters = {}
    matches_three = run_with_yaml("""
rules:
  test:
    conditions:
      callsign_prefix: N
      category: [A1, A2]
      min_vertical_rate: 200
    actions:
      callback: test_cb
""")

    assert matches_single > 0, "Single condition should match"
    assert matches_two > 0, "Two conditions should match"
    assert matches_three >= 0, "Three conditions may or may not match"
    assert matches_single >= matches_two, "Adding conditions should not increase matches"
    assert matches_two >= matches_three, "Adding more conditions should not increase matches"
