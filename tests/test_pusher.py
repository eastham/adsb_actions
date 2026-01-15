"""Integration test of the op_pusher/los.py module.  This test
actually calls the code that can push to the database, so make changes
with care..."""
import logging
import time
import yaml

from op_pusher.op_pusher_helpers import register_callbacks, enter_db_fake_mode, exit_workers
from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions

LOS_YAML_FILE = "src/op_pusher/rules.yaml"

JSON_STRING_PLANE1_NEAR = '{"now": 1661692178, "alt_baro": 5000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d1"}\n'
JSON_STRING_PLANE2_NEAR = '{"now": 1661692178, "alt_baro": 5000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d2"}\n'
JSON_STRING_PLANE2_LAND = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d2"}\n'
JSON_STRING_PLANE3_DELAY = '{"now": 1661692185, "alt_baro": 0, "gscp": 128, "lat": 41.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d3"}\n'

JSON_STRING_PLANE4_GROUND = '{"now": 1661692185, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_PLANE4_AIR = '{"now": 1661692185, "alt_baro": 4500, "gscp": 128, "lat": 40.748708, "lon": -119.2489313, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'

def test_pusher():
    Stats.reset()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    # load real-world YAML from file
    with open(LOS_YAML_FILE, 'rt', encoding="utf-8") as myfile:
        raw_yaml = myfile.read()
    yaml_data = yaml.safe_load(raw_yaml)

    adsb_actions = AdsbActions(yaml_data)
    register_callbacks(adsb_actions)
    enter_db_fake_mode()              # Caution, don't disable

    # put two airplanes in close proximity to test LOS processing
    adsb_actions.loop(JSON_STRING_PLANE1_NEAR)
    adsb_actions.loop(JSON_STRING_PLANE2_NEAR)
    adsb_actions.loop(JSON_STRING_PLANE3_DELAY)  # trigger LOS processing

    time.sleep(1) # delay for async handling
    assert Stats.los_add == 1
    time.sleep(1) # delay for gc loop
    assert Stats.los_finalize == 1

    # test non-local landing
    Stats.reset()
    adsb_actions.loop(JSON_STRING_PLANE2_NEAR)
    adsb_actions.loop(JSON_STRING_PLANE2_LAND)
    assert Stats.local_landings == 1

    # simulate aircraft appearing out of nowhere in the air
    # (these are treated as a takeoff according to the LOS_YAML_FILE rules)
    Stats.reset()
    adsb_actions.loop(JSON_STRING_PLANE4_AIR)
    assert Stats.takeoffs == 1
    assert Stats.popup_takeoffs == 1

    # simulate landing + takeoff
    adsb_actions.loop(JSON_STRING_PLANE4_GROUND)
    assert Stats.landings == 1
    assert Stats.local_landings == 1
    adsb_actions.loop(JSON_STRING_PLANE4_AIR)
    assert Stats.takeoffs == 2
    assert Stats.popup_takeoffs == 1  # no change

    exit_workers()
