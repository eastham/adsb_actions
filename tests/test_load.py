"""Load test w/ profiling support.

Use "pytest -s" to see profile output.  You may need to increase ITERATIONS
for clean profile output...
"""

import logging
import time
import cProfile

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
        callback: test_callback

    prox:
      conditions:
        min_alt: 3000
        max_alt: 10000
        regions: [ "Scenic", "Gerlach Corridor", "Empire/Razorback/Pattern", "Other" ]
        proximity: [ 400, .3 ] # alt sep in MSL, lateral sep in nm
      actions:
        callback: los_update_cb
"""

JSON_STRING_DISTANT = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 41.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_GROUND = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'

def test_load():
    ITERATIONS = 10000

    Stats.reset()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.WARNING)
    testinfra.set_all_loggers(logging.WARNING)

    yaml_data = yaml.safe_load(YAML_STRING)
    adsb_actions = AdsbActions(yaml_data)
    testinfra.setup_test_callback(adsb_actions)

    start_time = time.time()
    work_string = JSON_STRING_DISTANT+JSON_STRING_GROUND
    work_string = work_string * ITERATIONS

    # TODO centrally disable logger to prevent profile impact?
    with cProfile.Profile() as pr:
        pr.enable()

        adsb_actions.loop(work_string)
        pr.disable()
        pr.print_stats('tottime')

    assert Stats.callbacks_fired == ITERATIONS
    done_time = time.time()
    assert done_time - start_time < 5

    print(f"TIME ELAPSED: {done_time-start_time}")
