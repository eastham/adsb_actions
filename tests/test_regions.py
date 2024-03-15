"""Test for the "regions" condition."""

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
    banned: [ "N12345" ] 

  rules:
    banned_aircraft:
      conditions:
        aircraft_list: banned
        regions: ["Generic Gate Ground", "non-existent"]  # This is an OR expression
      actions:
        callback: ground_cb

    distant:
      conditions:
        regions: []
      actions:
        callback: distant_cb
"""

distant_cb_ctr = ground_cb_ctr = 0
def distant_cb(flight):
    global distant_cb_ctr
    distant_cb_ctr += 1

def ground_cb(flight):
    global ground_cb_ctr
    ground_cb_ctr += 1

JSON_STRING_DISTANT = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 41.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_GROUND = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'

def test_regions():
    Stats.reset()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    yaml_data = yaml.safe_load(YAML_STRING)
    adsb_actions = AdsbActions(yaml_data)
    testinfra.setup_test_callback(adsb_actions)
    adsb_actions.register_callback("distant_cb", distant_cb)
    adsb_actions.register_callback("ground_cb", ground_cb)

    adsb_actions.loop(JSON_STRING_DISTANT)
    assert distant_cb_ctr == 1
    assert ground_cb_ctr == 0

    adsb_actions.loop(JSON_STRING_GROUND)
    assert distant_cb_ctr == 1
    assert ground_cb_ctr == 1

    adsb_actions.loop(JSON_STRING_DISTANT)
    assert distant_cb_ctr == 2
    assert ground_cb_ctr == 1