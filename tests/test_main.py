import logging
import pytest

import yaml

import testinfra
from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions

YAML_STRING = """
  config:
    kmls:
      - tests/test1.kml 

  aircraft_lists:
    alert_aircraft: [ "N12345" ] 

  rules:
    alert_webhook:
      conditions:
        aircraft_list: alert_aircraft
      actions:
        webhook: True

    takeoff:
      conditions:
        transition_regions: [ "Generic Gate Ground", "Generic Gate Air" ]
      actions:
        callback: "test_callback"
        note: "saw_takeoff"
"""

JSON_STRING_GROUND = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'
JSON_STRING_AIR = '{"now": 1661692178, "alt_baro": 4500, "gscp": 128, "lat": 40.748708, "lon": -119.2489313, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'

@pytest.fixture
def adsb_state():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    Stats.reset()
    assert Stats.json_readlines == 0

    yaml_data = yaml.safe_load(YAML_STRING)
    adsb_actions = AdsbActions(yaml_data)
    testinfra.setup_test_callback(adsb_actions)

    yield adsb_actions

def test_main(adsb_state):
    adsb_state.loop(JSON_STRING_GROUND)

    assert Stats.json_readlines == 1
    assert Stats.condition_match_calls == 2
    assert Stats.callbacks_fired == 0

    adsb_state.loop(JSON_STRING_AIR)

    assert Stats.json_readlines == 2
    assert Stats.condition_match_calls == 4  # 2 per position
    assert Stats.callbacks_fired == 1

# test proper handling of non-US ICAO hex codes.
# Canada hex codes are supported, Mexico and others are not.
JSON_CANADA_GROUND = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "c07bed", "flight": "N12345"}'
JSON_CANADA_AIR = '{"now": 1661692178, "alt_baro": 5000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "c07bed", "flight": "N12345"}'
JSON_MEXICO_GROUND = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "0d08e6", "flight": "XA-VAA"}'
JSON_MEXICO_AIR = '{"now": 1661692178, "alt_baro": 5000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "0d08e6", "flight": "XA-VAA"}'

def test_foreign_icao(adsb_state):
    adsb_state.loop(JSON_CANADA_GROUND)
    adsb_state.loop(JSON_CANADA_AIR)
    assert Stats.callbacks_fired == 1

    adsb_state.loop(JSON_MEXICO_GROUND)
    adsb_state.loop(JSON_MEXICO_AIR)
    assert Stats.callbacks_fired == 2
