"Large test covering the 'transition_regions' rule condition."

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
    banned: [ "N4567", "N12345" ] 

  rules:
    takeoff_popup:
      conditions:
        transition_regions: [ ~, "Generic Gate Air" ]
      actions:
        callback: "test_callback"

    landing:
      conditions:
        transition_regions: [ "Generic Gate Air", "Generic Gate Ground" ]
      actions:
        callback: "test_callback"
"""

JSON_STRING_DISTANT = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 41.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_GROUND = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_AIR = '{"now": 1661692178, "alt_baro": 4500, "gscp": 128, "lat": 40.748708, "lon": -119.2489313, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'

def test_transitions():
    Stats.reset()
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    yaml_data = yaml.safe_load(YAML_STRING)
    adsb_actions = AdsbActions(yaml_data)
    testinfra.setup_test_callback(adsb_actions)

    adsb_actions.loop((JSON_STRING_DISTANT+'\n')*3)
    assert Stats.callbacks_fired == 0

    adsb_actions.loop((JSON_STRING_AIR+'\n')*3)
    assert Stats.callbacks_fired == 1
    assert Stats.last_callback_flight.is_in_bboxes(['Generic Gate Air'])

    adsb_actions.loop((JSON_STRING_GROUND+'\n')*3)
    assert Stats.callbacks_fired == 2
    assert Stats.last_callback_flight.is_in_bboxes(['Generic Gate Ground'])
