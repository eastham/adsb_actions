import logging

import yaml

from stats import Stats
import main
import rules
import testinfra

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
        cooldown: 180 # minutes
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
    f = main.setup_flights(yaml_data)
    r = rules.Rules(yaml_data)

    testinfra.process_adsb(JSON_STRING, f, r)
    assert Stats.callbacks_fired == 1

    testinfra.process_adsb(JSON_STRING_100S_LATER, f, r)
    assert Stats.callbacks_fired == 1 # no new callbacks due to cooldown

    testinfra.process_adsb(JSON_STRING_20000S_LATER, f, r)
    assert Stats.callbacks_fired == 2
