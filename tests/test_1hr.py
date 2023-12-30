"""
Huge integration test that takes an hour of busy airspace data 
and checks that we're detecting the right number of takeoffs/landings/local flights.
"""

import logging

import yaml

from stats import Stats
from adsbactions import AdsbActions

YAML_STRING= """
  config:
    kmls:
      - tests/test1.kml 

  rules:
    takeoff:
      conditions:
        transition_regions: [ "Generic Gate Ground", "Generic Gate Air" ]
      actions:
        slack: true
        note: "saw_takeoff"

    landing:
      conditions:
        transition_regions: [ "Generic Gate Air", "Generic Gate Ground" ]
      actions:
        page: true
        callback: "test_callback"
"""

JSON_STRING1 = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'
JSON_STRING2 = '{"now": 1661692178, "alt_baro": 4500, "gscp": 128, "lat": 40.748708, "lon": -119.2489313, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'

def test_main():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    Stats.reset()
    yaml_data = yaml.safe_load(YAML_STRING)

    with open("tests/1hr.json", 'rt', encoding="utf-8") as myfile:
        json_data = myfile.read()

    adsb_actions = AdsbActions(yaml_data)
    adsb_actions.loop(json_data)

    assert Stats.slacks_fired == 14 # takeoffs
    assert Stats.pages_fired == 18  # landings
    assert Stats.callbacks_with_notes == 9  # landings from local flights

