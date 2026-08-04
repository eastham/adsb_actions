"""Test for the 'changed_regions' condition in both strict and any modes."""

import logging
import yaml
import pytest

from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions

# Test locations
DISTANT = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 41.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
GROUND = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
AIR = '{"now": 1661692178, "alt_baro": 4500, "gscp": 128, "lat": 40.748708, "lon": -119.2489313, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'


def make_yaml(mode):
    return f"""
      config:
        kmls:
          - tests/test1.kml
      rules:
        region_change:
          conditions:
            changed_regions: {mode}
          actions:
            callback: cb
    """


@pytest.fixture
def callback_counter():
    return {"count": 0}


@pytest.fixture
def make_adsb_actions(callback_counter):
    def _make(mode):
        Stats.reset()
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
        yaml_data = yaml.safe_load(make_yaml(mode))
        aa = AdsbActions(yaml_data)
        aa.register_callback("cb", lambda _: callback_counter.__setitem__("count", callback_counter["count"] + 1))
        return aa
    return _make


@pytest.mark.parametrize("mode,sequence,expected_counts", [
    # "any" mode: triggers on any region change including to/from None
    ("any", [DISTANT, DISTANT, GROUND, GROUND, AIR, DISTANT], [0, 0, 1, 1, 2, 3]),
    # "strict" mode: only triggers when both prev and current are in some region
    ("strict", [DISTANT, DISTANT, GROUND, GROUND, AIR, GROUND, DISTANT], [0, 0, 0, 0, 1, 2, 2]),
    # "true" (backwards compat) behaves like "any"
    ("true", [DISTANT, GROUND, AIR, DISTANT], [0, 1, 2, 3]),
])
def test_changed_regions(make_adsb_actions, callback_counter, mode, sequence, expected_counts):
    aa = make_adsb_actions(mode)
    for position, expected in zip(sequence, expected_counts):
        aa.loop(position)
        assert callback_counter["count"] == expected, f"After position {sequence.index(position)}"
