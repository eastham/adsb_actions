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

  aircraft_lists:  # this is probably not the right way to do this
    banned: [ "N42PE", "N12345" ] 

  rules:
    banned_slack:
      conditions:
        aircraft_list: banned
      actions:
        slack: True
        page: True

    takeoff:
      conditions:
        transition_regions: [ "Generic Gate Ground", "Generic Gate Air" ]
      actions:
        callback: "add_op"
        note: "saw_takeoff"
"""

JSON_STRING1 = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'
JSON_STRING2 = '{"now": 1661692178, "alt_baro": 4500, "gscp": 128, "lat": 40.748708, "lon": -119.2489313, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'

def test_main():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    Stats.reset()
    assert Stats.json_readlines == 0

    yaml_data = yaml.safe_load(YAML_STRING)
    flights = main.setup_flights(yaml_data)
    rules_instance = rules.Rules(yaml_data)

    testinfra.process_adsb(JSON_STRING1, flights, rules_instance)

    assert Stats.json_readlines == 1
    assert Stats.condition_match_calls == 2
    assert Stats.condition_matches_true == 1
    assert Stats.callbacks_fired == 0

    testinfra.process_adsb(JSON_STRING2, flights, rules_instance)

    assert Stats.json_readlines == 2
    assert Stats.condition_match_calls == 4  # 2 per position
    assert Stats.condition_matches_true == 3  # 1 the first positon, 2 second
    assert Stats.callbacks_fired == 1