import logging
import time
import cProfile

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

def test_load():
    ITERATIONS = 100

    Stats.reset()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.WARNING)

    yaml_data = yaml.safe_load(YAML_STRING)
    f = main.setup_flights(yaml_data)
    r = rules.Rules(yaml_data)

    start_time = time.time()
    work_string = JSON_STRING_DISTANT+JSON_STRING_GROUND
    work_string = work_string * ITERATIONS

    # XXX centrally disable logger?
    # NOTE: use pytest -s to see profile output
    with cProfile.Profile() as pr:
        pr.enable()

        testinfra.process_adsb(work_string, f, r)
        pr.disable()
        pr.print_stats('tottime')

    assert Stats.callbacks_fired == ITERATIONS
    done_time = time.time()
    assert done_time - start_time < 5

    print(f"TIME ELAPSED: {done_time-start_time}")
