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
        regions: ["Generic Gate Ground", "non-existent"]  # This is an OR expression
      actions:
        callback: test_callback
"""

JSON_STRING_DISTANT = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 41.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_GROUND = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'

def test_cooldown():
    Stats.reset()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    yaml_data = yaml.safe_load(YAML_STRING)
    f = main.setup_flights(yaml_data)
    r = rules.Rules(yaml_data)

    testinfra.process_adsb(JSON_STRING_DISTANT, f, r)
    assert Stats.callbacks_fired == 0

    testinfra.process_adsb(JSON_STRING_GROUND, f, r)
    assert Stats.callbacks_fired == 1 
