"""Test for min_alt/max_alt conditions."""
import logging
import yaml

from stats import Stats
from adsbactions import AdsbActions
import testinfra

YAML_STRING = """
  config:
    kmls:
      - tests/test1.kml 

  aircraft_lists:
    banned: [ "N12345" ] 

  rules:
    block:
      conditions:
        min_alt: 4000
        max_alt: 10000
      actions:
        callback: alt_cb
    
    ceiling:
      conditions:
        max_alt: 10000
      actions:
        callback: alt_cb

    floor:
      conditions:
        min_alt: 4000
      actions:
        callback: alt_cb
"""

alt_cb_ctr = ground_cb_ctr = 0
def alt_cb(flight):
    global alt_cb_ctr
    alt_cb_ctr += 1

def ground_cb(flight):
    global ground_cb_ctr
    ground_cb_ctr += 1

JSON_STRING_3000 = '{"now": 1661692178, "alt_baro": 3000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_4000 = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_5000 = '{"now": 1661692178, "alt_baro": 5000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_11000 = '{"now": 1661692178, "alt_baro": 11000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'

def test_altitudes():
    Stats.reset()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    yaml_data = yaml.safe_load(YAML_STRING)
    adsb_actions = AdsbActions(yaml_data)
    testinfra.setup_test_callback(adsb_actions)
    adsb_actions.register_callback("alt_cb", alt_cb)
    adsb_actions.register_callback("ground_cb", ground_cb)

    adsb_actions.loop(JSON_STRING_3000)
    assert alt_cb_ctr == 1

    adsb_actions.loop(JSON_STRING_4000)
    assert alt_cb_ctr == 4

    adsb_actions.loop(JSON_STRING_5000)
    assert alt_cb_ctr == 7

    adsb_actions.loop(JSON_STRING_11000)
    assert alt_cb_ctr == 8
