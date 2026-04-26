"""Large test for proximity rules and expiration."""
import logging
import yaml

from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions

YAML_STRING = """
  config:
    kmls:
      - tests/test2.kml
      - tests/test3.kml

  rules:
    ui_update:
      conditions:
        regions: [ "Scenic", "Gerlach Corridor", "Empire/Razorback/Pattern", "Other" ]
      actions:
        callback: aircraft_update_cb

    ui_remove:
      conditions:
        regions: ~
      actions:
        callback: aircraft_remove_cb

    prox:
        conditions:
            min_alt: 3000
            max_alt: 10000
            regions: [ "Scenic", "Gerlach Corridor", "Empire/Razorback/Pattern", "Other" ]
            proximity: [ 400, .3 ] # alt sep in MSL, lateral sep in nm
        actions:
            callback: los_update_cb
"""

aircraft_update_ctr = aircraft_remove_ctr = los_update_ctr = 0
def aircraft_update_cb(flight):
    global aircraft_update_ctr
    aircraft_update_ctr += 1

def aircraft_remove_cb(flight):
    global aircraft_remove_ctr
    aircraft_remove_ctr += 1

def los_update_cb(flight1, flight2):
    global los_update_ctr
    logging.info(f"got los_update_cb {flight1.flight_id} {flight2.flight_id}")
    los_update_ctr += 1

JSON_STRING_PLANE1_ZEROALT = '{"now": 1661692178, "alt_baro": 0, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d1"}\n'
JSON_STRING_PLANE1_DISTANT = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 41.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d1"}\n'
JSON_STRING_PLANE1_NEAR = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d1"}\n'
JSON_STRING_PLANE2_NEAR = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d2"}\n'
JSON_STRING_PLANE3_DELAY = '{"now": 1661692183, "alt_baro": 0, "gscp": 128, "lat": 41.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d3"}\n'
JSON_STRING_PLANE4_TOOFAR = '{"now": 1661692178, "alt_baro": 4300, "gscp": 128, "lat": 40.76864689708049, "lon": -119.20915027077689, "track": 203.4, "hex": "a061d4"}\n'
JSON_STRING_PLANE5_WITHINPROX = '{"now": 1661692178, "alt_baro": 4300, "gscp": 128, "lat": 40.76759089177806, "lon":  -119.20984743421535, "track": 203.4, "hex": "a061d5"}\n'
JSON_STRING_PLANE6_TOOFAR_ALT = '{"now": 1661692178, "alt_baro": 4800, "gscp": 128, "lat": 40.76759089177806, "lon":  -119.20984743421535, "track": 203.4, "hex": "a061d6"}\n'

def test_prox():
    Stats.reset()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    yaml_data = yaml.safe_load(YAML_STRING)

    adsb_actions = AdsbActions(yaml_data)
    adsb_actions.register_callback("aircraft_update_cb", aircraft_update_cb)
    adsb_actions.register_callback("aircraft_remove_cb", aircraft_remove_cb)
    adsb_actions.register_callback("los_update_cb", los_update_cb)

    basic_prox_test(adsb_actions)

    big_prox_test(adsb_actions)

def basic_prox_test(adsb_actions):
    adsb_actions.loop(JSON_STRING_PLANE1_NEAR)
    assert aircraft_update_ctr == 1
    # causes plane to go out of view
    adsb_actions.loop(JSON_STRING_PLANE1_ZEROALT)
    assert aircraft_remove_ctr == 1

    # set up aircraft in the same vicinity for prox testing
    adsb_actions.loop(JSON_STRING_PLANE1_NEAR)
    adsb_actions.loop(JSON_STRING_PLANE4_TOOFAR)
    adsb_actions.loop(JSON_STRING_PLANE6_TOOFAR_ALT)
    adsb_actions.loop(JSON_STRING_PLANE5_WITHINPROX)
    adsb_actions.loop(JSON_STRING_PLANE3_DELAY) # allow time to pass for checkpoint
    assert los_update_ctr == 3  # PLANE1 is near PLANE5, PLANE5 is near PLANE4

def big_prox_test(adsb_actions):
    with open("tests/20minutes.json", 'rt', encoding="utf-8") as myfile:
        json_data = myfile.read()

    adsb_actions.loop(json_data)

    # check the number of aircraft left visible after expiry etc
    rendered_flight_ctr = 0
    for f in adsb_actions.flights.flight_dict.values():
        rendered_flight_ctr += 1 if f.in_any_bbox() else 0
    assert rendered_flight_ctr == 4  # unexpired aircraft left
    assert los_update_ctr == 17


# Resampling test aircraft - they will cross each other's path in the middle
# one aircraft varying in latitude only
JSON_STRING_PLANE11_BEFORE = '{"now": 1661692168, "alt_baro": 4000, "gscp": 128, "lat": 39.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d2"}\n'
JSON_STRING_PLANE11_AFTER = '{"now": 1661692188, "alt_baro": 4000, "gscp": 128, "lat": 41.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d2"}\n'
# one aircraft varying in longitude only
JSON_STRING_PLANE12_BEFORE = '{"now": 1661692168, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -118.2122323, "track": 203.4, "hex": "a061d3"}\n'
JSON_STRING_PLANE12_AFTER = '{"now": 1661692188, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -120.2122323, "track": 203.4, "hex": "a061d3"}\n'
# one aircraft in the middle that never moves
JSON_STRING_PLANE13_CENTRAL = '{"now": 1661692168, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d4"}\n'


def test_resampling_prox():
    Stats.reset()
    global los_update_ctr, JSON_STRING_PLANE11_BEFORE
    los_update_ctr = 0

    logging.basicConfig(format='%(levelname)s: %(message)s',
                        level=logging.DEBUG)
    logging.info('System started.')

    logging.info("*** resampling test 1 ***")
    yaml_data = yaml.safe_load(YAML_STRING)
    run_crossing_prox_test(yaml_data)

    assert los_update_ctr == 2  # one for each aircraft

    logging.info("*** resampling test 2 ***")
    # vary time offsets of the test aircraft so they don't intersect.
    JSON_STRING_PLANE11_BEFORE = '{"now": 1661692167, "alt_baro": 4000, "gscp": 128, "lat": 39.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d2"}\n'
    run_crossing_prox_test(yaml_data)
    assert los_update_ctr == 2  # no change

    logging.info("*** resampling test 3 ***")
    run_singlepoint_prox_test(yaml_data)
    assert los_update_ctr == 2  # central plane expires before crossing with EXPIRE_TIME=5

    logging.info("*** resampling test 4 ***")
    run_singlepoint_prox_test_with_expiry(yaml_data)
    assert los_update_ctr == 2  # no change

def run_crossing_prox_test(yaml_data):
    adsb_actions = AdsbActions(yaml_data, resample=True)
    adsb_actions.register_callback("los_update_cb", los_update_cb)

    adsb_actions.loop(JSON_STRING_PLANE11_BEFORE)
    adsb_actions.loop(JSON_STRING_PLANE11_AFTER)
    adsb_actions.loop(JSON_STRING_PLANE12_BEFORE)
    adsb_actions.loop(JSON_STRING_PLANE12_AFTER)

    adsb_actions.do_resampled_prox_checks(gc_callback=None)

def run_singlepoint_prox_test(yaml_data):
    # plane 13 in the middle with only one data point, plane 12 crosses
    adsb_actions = AdsbActions(yaml_data, resample=True)
    adsb_actions.register_callback("los_update_cb", los_update_cb)

    adsb_actions.loop(JSON_STRING_PLANE12_BEFORE)
    adsb_actions.loop(JSON_STRING_PLANE13_CENTRAL)
    adsb_actions.loop(JSON_STRING_PLANE12_AFTER)

    adsb_actions.do_resampled_prox_checks(gc_callback=None)

def run_singlepoint_prox_test_with_expiry(yaml_data):
    # same as above but the central plane should be expired before other one crosses
    OLD_CENTRAL_PLANE = '{"now": 1661692008, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d4"}\n'

    adsb_actions = AdsbActions(yaml_data, resample=True)
    adsb_actions.register_callback("los_update_cb", los_update_cb)

    adsb_actions.loop(JSON_STRING_PLANE12_BEFORE)
    adsb_actions.loop(OLD_CENTRAL_PLANE)
    adsb_actions.loop(JSON_STRING_PLANE12_AFTER)

    adsb_actions.do_resampled_prox_checks(gc_callback=None)


# one_in_aircraft_list tests: PLANE1 is "participating", PLANE5 is not.
# PLANE1 and PLANE5 are within proximity range of each other (reusing coords above).
YAML_ONE_IN_LIST = """
  config:
    kmls:
      - tests/test2.kml

  aircraft_lists:
    # ICAO a061d1 -> N1234X, a061d7 -> N12343
    participating: ["N1234X", "N12343"]

  rules:
    prox:
      conditions:
        min_alt: 3000
        max_alt: 10000
        regions: [ "Scenic", "Gerlach Corridor", "Empire/Razorback/Pattern", "Other" ]
        proximity: [ 400, .3 ]
        one_in_aircraft_list: participating
      actions:
        callback: los_update_cb
"""

# PLANE1 has flight "N61D1" (participating), PLANE5 has no flight field (non-participating).
# Both are in proximity range of each other.
JSON_OIL_PLANE1 = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d1", "flight": "N61D1   "}\n'
JSON_OIL_PLANE5 = '{"now": 1661692178, "alt_baro": 4300, "gscp": 128, "lat": 40.76759089177806, "lon": -119.20984743421535, "track": 203.4, "hex": "a061d5", "flight": "N99999  "}\n'
# A second non-participating plane, also in proximity range of PLANE1
JSON_OIL_PLANE6 = '{"now": 1661692178, "alt_baro": 4300, "gscp": 128, "lat": 40.76759089177806, "lon": -119.20984743421535, "track": 203.4, "hex": "a061d6", "flight": "N88888  "}\n'
# A second participating plane (hex a061d7 -> N12343) — pair of two participating should NOT fire
JSON_OIL_PLANE7 = '{"now": 1661692178, "alt_baro": 4300, "gscp": 128, "lat": 40.76759089177806, "lon": -119.20984743421535, "track": 203.4, "hex": "a061d7"}\n'
# Trigger must be within MIN_FRESH (5s) of the test planes (t=178), and >= CHECKPOINT_INTERVAL (5s) after last_checkpoint.
# Use t+5 exactly: 1661692183 is 5s after t=178, satisfying both constraints.
JSON_OIL_TRIGGER = '{"now": 1661692183, "alt_baro": 0, "gscp": 128, "lat": 41.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d3"}\n'

def test_one_in_aircraft_list():
    """one_in_aircraft_list fires when exactly one aircraft in the pair is in the list."""
    Stats.reset()
    global los_update_ctr
    los_update_ctr = 0

    yaml_data = yaml.safe_load(YAML_ONE_IN_LIST)
    adsb = AdsbActions(yaml_data)
    adsb.register_callback("los_update_cb", los_update_cb)

    # participating + non-participating => should fire
    adsb.loop(JSON_OIL_PLANE1)
    adsb.loop(JSON_OIL_PLANE5)
    adsb.loop(JSON_OIL_TRIGGER)
    assert los_update_ctr >= 1, "Expected event: one participating + one non-participating"

    # two non-participating planes in proximity => should NOT fire
    Stats.reset()
    los_update_ctr = 0
    adsb2 = AdsbActions(yaml_data)
    adsb2.register_callback("los_update_cb", los_update_cb)
    adsb2.loop(JSON_OIL_PLANE5)
    adsb2.loop(JSON_OIL_PLANE6)
    adsb2.loop(JSON_OIL_TRIGGER)
    assert los_update_ctr == 0, "Expected no event: both planes non-participating"

    # two participating planes in proximity => should NOT fire
    Stats.reset()
    los_update_ctr = 0
    adsb3 = AdsbActions(yaml_data)
    adsb3.register_callback("los_update_cb", los_update_cb)
    adsb3.loop(JSON_OIL_PLANE1)
    adsb3.loop(JSON_OIL_PLANE7)
    adsb3.loop(JSON_OIL_TRIGGER)
    assert los_update_ctr == 0, "Expected no event: both planes participating"
