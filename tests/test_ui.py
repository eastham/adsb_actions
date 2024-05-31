"""Test for functionality needed to support a UI"""
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
            callback: abe_update_cb
"""

aircraft_update_ctr = aircraft_remove_ctr = abe_update_ctr = 0
def aircraft_update_cb(flight):
    global aircraft_update_ctr
    aircraft_update_ctr += 1

def aircraft_remove_cb(flight):
    global aircraft_remove_ctr
    aircraft_remove_ctr += 1

def abe_update_cb(flight1, flight2):
    global abe_update_ctr
    abe_update_ctr += 1

JSON_STRING_3000 = '{"now": 1661692178, "alt_baro": 3000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9"} \n'
JSON_STRING_4000 = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9"} \n'
JSON_STRING_5000 = '{"now": 1661692178, "alt_baro": 5000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9"} \n'
JSON_STRING_11000 = '{"now": 1661692178, "alt_baro": 11000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9"} \n'
JSON_STRING_DISTANT = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 41.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9"} \n'
JSON_STRING_GROUND = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9"} \n'
JSON_STRING_GROUND_PLANE2 = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061db"} \n'
JSON_STRING_ZEROALT = '{"now": 1661692178, "alt_baro": 0, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9"} \n'
JSON_STRING_PLANE3_DELAY = '{"now": 1661692185, "alt_baro": 0, "gscp": 128, "lat": 41.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061da"}\n'

def test_ui():
    Stats.reset()

    #logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    yaml_data = yaml.safe_load(YAML_STRING)

    with open("tests/20minutes.json", 'rt', encoding="utf-8") as myfile:
        json_data = myfile.read()

    adsb_actions = AdsbActions(yaml_data)
    adsb_actions.register_callback("aircraft_update_cb", aircraft_update_cb)
    adsb_actions.register_callback("aircraft_remove_cb", aircraft_remove_cb)
    adsb_actions.register_callback("abe_update_cb", abe_update_cb)

    adsb_actions.loop(JSON_STRING_GROUND)
    assert aircraft_update_ctr == 1
    adsb_actions.loop(JSON_STRING_ZEROALT)
    assert aircraft_remove_ctr == 1

    # set up two aircraft in the same position for prox testing
    adsb_actions.loop(JSON_STRING_GROUND)
    adsb_actions.loop(JSON_STRING_GROUND_PLANE2)
    adsb_actions.loop(JSON_STRING_PLANE3_DELAY) # allow time to pass for checkpoint
    assert abe_update_ctr == 2  # one for each plane near the other

    if True:
        adsb_actions.loop(json_data)

        # check the number of aircraft left visible after expiry etc
        rendered_flight_ctr = 0
        for f in adsb_actions.flights.flight_dict.values():
            rendered_flight_ctr += 1 if f.in_any_bbox() else 0
        assert rendered_flight_ctr == 4
