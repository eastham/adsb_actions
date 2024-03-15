"""Test for the "latlongring" condition."""

import logging
import yaml

from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions

YAML_STRING = """
  config:
    kmls:  # optional KML files that specify geographic regions.
      - tests/test3.kml 

  aircraft_lists:
    alert_aircraft: [ "N12345" ] # optional lists of tail numbers of interest.

  rules:
    nearby:
      conditions: 
        min_alt: 4000        # feet MSL, must be >= to match
        max_alt: 10000       # feel MSL, must be <= to match
        aircraft_list: alert_aircraft  # use aircraft_list above
        latlongring: [20, 40.763537, -119.2122323]
      actions:
        callback: t1_cb

    readme_rule:
      conditions: 
        min_alt: 4000        # feet MSL, must be >= to match
        max_alt: 10000       # feel MSL, must be <= to match
        aircraft_list: alert_aircraft  # use aircraft_list above
        latlongring: [20, 40.763537, -119.2122323]
        regions: [ "23 upwind" ]
      actions:
        callback: t2_cb
"""
t1_cb_ctr = 0
def t1_cb(flight):
    global t1_cb_ctr
    t1_cb_ctr += 1

t2_cb_ctr = 0
def t2_cb(flight):
    global t2_cb_ctr
    t2_cb_ctr += 1

JSON_STRING_NEARBY = '{"now": 1661692178, "alt_baro": 5000, "gscp": 128, "lat": 40.751249, "lon": -119.249305, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_BEYOND_REGION_ONLY = '{"now": 1661692178, "alt_baro": 5000, "gscp": 128, "lat": 40.781148, "lon": -119.2449769, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_DISTANT = '{"now": 1661692178, "alt_baro": 5000, "gscp": 128, "lat": 40.5819728, "lon": -119.6232779, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'

def test_latlong():
    Stats.reset()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    yaml_data = yaml.safe_load(YAML_STRING)
    adsb_actions = AdsbActions(yaml_data)
    adsb_actions.register_callback("t1_cb", t1_cb)
    adsb_actions.register_callback("t2_cb", t2_cb)

    adsb_actions.loop(JSON_STRING_NEARBY)
    assert t1_cb_ctr == 1
    assert t2_cb_ctr == 1

    adsb_actions.loop(JSON_STRING_BEYOND_REGION_ONLY)
    assert t1_cb_ctr == 2
    assert t2_cb_ctr == 1

    adsb_actions.loop(JSON_STRING_DISTANT)
    assert t1_cb_ctr == 2
    assert t2_cb_ctr == 1
