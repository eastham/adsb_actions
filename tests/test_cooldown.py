"""Test of the "cooldown" condition."""
import logging

import yaml

import testinfra
from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions

YAML_STRING = """
  config:
    kmls:
      - tests/test1.kml 

  aircraft_lists:
    watched: [ "N12345" ]

  rules:
    watched_aircraft:
      conditions:
        aircraft_list: watched
        cooldown: 3     # minutes
      actions:
        callback: test_callback
"""

JSON_STRING = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_100S_LATER = '{"now": 1661692278, "alt_baro": 4500, "gscp": 128, "lat": 40.748708, "lon": -119.2489313, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'
JSON_STRING_20000S_LATER = '{"now": 1661712278, "alt_baro": 4500, "gscp": 128, "lat": 40.748708, "lon": -119.2489313, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'

def test_cooldown():
    Stats.reset()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    yaml_data = yaml.safe_load(YAML_STRING)
    adsb_actions = AdsbActions(yaml_data)
    testinfra.setup_test_callback(adsb_actions)

    adsb_actions.loop(JSON_STRING)
    assert Stats.callbacks_fired == 1

    adsb_actions.loop(JSON_STRING_100S_LATER)
    assert Stats.callbacks_fired == 1 # no new callbacks due to cooldown

    adsb_actions.loop(JSON_STRING_20000S_LATER)
    assert Stats.callbacks_fired == 2
