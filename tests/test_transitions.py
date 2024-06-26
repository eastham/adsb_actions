"Large test covering the 'transition_regions' rule condition."

import logging
import yaml
import pytest

import testinfra
from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions

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
        callback: "takeoff_callback"

    takeoff:
      conditions:
        transition_regions: [ "Generic Gate Ground", "Generic Gate Air" ]
      actions:
        callback: "takeoff_callback"

    landing:
      conditions:
        transition_regions: [ "Generic Gate Air", "Generic Gate Ground" ]
      actions:
        callback: "landing_callback"
"""

JSON_STRING_DISTANT = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 41.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_GROUND = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_AIR = '{"now": 1661692178, "alt_baro": 4500, "gscp": 128, "lat": 40.748708, "lon": -119.2489313, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'

@pytest.fixture
def adsb_actions():
    logging.basicConfig(format='%(levelname)s: %(message)s',
                        level=logging.DEBUG)
    testinfra.set_all_loggers(logging.DEBUG)
    logging.info('System started.')
    Stats.reset()

    yaml_data = yaml.safe_load(YAML_STRING)
    adsb_actions = AdsbActions(yaml_data)
    adsb_actions.register_callback("takeoff_callback", takeoff_callback)
    adsb_actions.register_callback("landing_callback", landing_callback)
    yield adsb_actions

takeoff_callback_cb_ctr = landing_callback_cb_ctr = 0

def takeoff_callback(flight):
    global takeoff_callback_cb_ctr
    takeoff_callback_cb_ctr += 1

def landing_callback(flight):
    global landing_callback_cb_ctr
    landing_callback_cb_ctr += 1

def test_transitions(adsb_actions):
    global takeoff_callback_cb_ctr, landing_callback_cb_ctr

    # "popup" takeoff with no prior activity / bboxes
    adsb_actions.loop((JSON_STRING_AIR+'\n')*3)
    assert Stats.callbacks_fired == 1
    assert Stats.last_callback_flight.is_in_bboxes(['Generic Gate Air'])
    assert takeoff_callback_cb_ctr == 1

    # landing
    adsb_actions.loop((JSON_STRING_GROUND+'\n')*3)
    assert Stats.callbacks_fired == 2
    assert Stats.last_callback_flight.is_in_bboxes(['Generic Gate Ground'])
    assert landing_callback_cb_ctr == 1

    # takeoff again
    adsb_actions.loop((JSON_STRING_AIR+'\n')*3)
    assert Stats.callbacks_fired == 3
    assert Stats.last_callback_flight.is_in_bboxes(['Generic Gate Air'])
    assert takeoff_callback_cb_ctr == 2

    # popup again, case where aircraft is seen, but in no bboxes prior.
    # arguable if this rule is formulated correctly, but checking the logic...
    adsb_actions.loop((JSON_STRING_DISTANT+'\n')*3)
    assert not Stats.last_callback_flight.in_any_bbox()
    adsb_actions.loop((JSON_STRING_AIR+'\n')*3)
    assert Stats.callbacks_fired == 4
    assert takeoff_callback_cb_ctr == 3
