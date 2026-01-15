"""Test unconditional rules (conditions: {} or conditions: [])."""

import logging
import pytest
import yaml

import testinfra
from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions

JSON_STRING = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'


def make_yaml(conditions):
    return f"""
      config:
        kmls: [tests/test1.kml]
      rules:
        unconditional_rule:
          conditions: {conditions}
          actions:
            callback: test_callback
    """


def test_unconditional_empty_dict():
    """conditions: {{}} creates an unconditional rule that always fires."""
    Stats.reset()
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    aa = AdsbActions(yaml.safe_load(make_yaml("{}")))
    testinfra.setup_test_callback(aa)

    aa.loop(JSON_STRING)
    assert Stats.callbacks_fired == 1

    aa.loop(JSON_STRING)
    assert Stats.callbacks_fired == 2


def test_unconditional_empty_list_fails():
    """conditions: [] raises an error during validation."""
    Stats.reset()
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    with pytest.raises(AttributeError):
        AdsbActions(yaml.safe_load(make_yaml("[]")))
