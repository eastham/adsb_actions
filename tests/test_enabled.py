"""Test the 'enabled' condition."""

import logging
import yaml

import testinfra
from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions

YAML_ENABLED_TRUE = """
  config:
    kmls:
      - tests/test1.kml

  rules:
    test_enabled_true:
      conditions:
        enabled: True
      actions:
        callback: enabled_cb
"""

YAML_ENABLED_FALSE = """
  config:
    kmls:
      - tests/test1.kml

  rules:
    test_enabled_false:
      conditions:
        enabled: False
      actions:
        callback: enabled_cb
"""

YAML_ENABLED_WITH_CONDITIONS = """
  config:
    kmls:
      - tests/test1.kml

  rules:
    test_enabled_true_with_alt:
      conditions:
        enabled: True
        min_alt: 4000
      actions:
        callback: enabled_cb

    test_enabled_false_with_alt:
      conditions:
        enabled: False
        min_alt: 4000
      actions:
        callback: enabled_cb
"""

YAML_ENABLED_OMITTED = """
  config:
    kmls:
      - tests/test1.kml

  rules:
    test_no_enabled:
      conditions:
        min_alt: 4000
      actions:
        callback: enabled_cb
"""

enabled_cb_ctr = 0
def enabled_cb(flight):
    global enabled_cb_ctr
    enabled_cb_ctr += 1

JSON_STRING_5000 = '{"now": 1661692178, "alt_baro": 5000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'


def test_enabled_true():
    """Test that enabled: True allows the rule to match."""
    global enabled_cb_ctr
    enabled_cb_ctr = 0
    Stats.reset()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    yaml_data = yaml.safe_load(YAML_ENABLED_TRUE)
    adsb_actions = AdsbActions(yaml_data)
    testinfra.setup_test_callback(adsb_actions)
    adsb_actions.register_callback("enabled_cb", enabled_cb)

    adsb_actions.loop(JSON_STRING_5000)
    # Callback should fire when enabled is True
    assert enabled_cb_ctr == 1


def test_enabled_false():
    """Test that enabled: False prevents the rule from matching."""
    global enabled_cb_ctr
    enabled_cb_ctr = 0
    Stats.reset()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    yaml_data = yaml.safe_load(YAML_ENABLED_FALSE)
    adsb_actions = AdsbActions(yaml_data)
    testinfra.setup_test_callback(adsb_actions)
    adsb_actions.register_callback("enabled_cb", enabled_cb)

    adsb_actions.loop(JSON_STRING_5000)
    # Callback should NOT fire when enabled is False
    assert enabled_cb_ctr == 0


def test_enabled_with_other_conditions():
    """Test that enabled works in combination with other conditions."""
    global enabled_cb_ctr
    enabled_cb_ctr = 0
    Stats.reset()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    yaml_data = yaml.safe_load(YAML_ENABLED_WITH_CONDITIONS)
    adsb_actions = AdsbActions(yaml_data)
    testinfra.setup_test_callback(adsb_actions)
    adsb_actions.register_callback("enabled_cb", enabled_cb)

    adsb_actions.loop(JSON_STRING_5000)
    # Only the rule with enabled: True should fire (altitude condition is met for both)
    assert enabled_cb_ctr == 1


def test_enabled_omitted():
    """Test that omitting the enabled condition allows the rule to match (default behavior)."""
    global enabled_cb_ctr
    enabled_cb_ctr = 0
    Stats.reset()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    yaml_data = yaml.safe_load(YAML_ENABLED_OMITTED)
    adsb_actions = AdsbActions(yaml_data)
    testinfra.setup_test_callback(adsb_actions)
    adsb_actions.register_callback("enabled_cb", enabled_cb)

    adsb_actions.loop(JSON_STRING_5000)
    # Callback should fire when enabled is omitted (default behavior)
    assert enabled_cb_ctr == 1
