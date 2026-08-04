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
    watched: [ "N4567", "N12345" ] 

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


YAML_TWO_KMLS = """
  config:
    kmls:
      - tests/test1.kml
      - tests/test2.kml

  rules:
    takeoff_popup:
      conditions:
        transition_regions: [ ~, "Generic Gate Air" ]
      actions:
        callback: "takeoff_callback"

    kml_scoped_popup:
      conditions:
        transition_regions: [ "~test2.kml", "Generic Gate Air" ]
      actions:
        callback: "kml_scoped_callback"
"""

# Inside test2.kml "Scenic" region (alt 4500-7000), outside test1.kml regions
JSON_STRING_SCENIC = '{"now": 1661692178, "alt_baro": 5000, "gscp": 128, "lat": 40.79, "lon": -119.22, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'

@pytest.fixture
def adsb_actions_two_kmls():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    testinfra.set_all_loggers(logging.DEBUG)
    Stats.reset()
    yaml_data = yaml.safe_load(YAML_TWO_KMLS)
    aa = AdsbActions(yaml_data)
    aa.register_callback("takeoff_callback", takeoff_callback)
    aa.register_callback("kml_scoped_callback", kml_scoped_callback)
    yield aa

kml_scoped_cb_ctr = 0

def kml_scoped_callback(flight):
    global kml_scoped_cb_ctr
    kml_scoped_cb_ctr += 1

def test_two_kmls_popup_not_broken(adsb_actions_two_kmls):
    """Adding a second KML must not break the ~ (popup) rule."""
    global takeoff_callback_cb_ctr
    takeoff_callback_cb_ctr = 0

    # popup into air region — was in no region across both KMLs
    adsb_actions_two_kmls.loop((JSON_STRING_AIR+'\n')*3)
    assert takeoff_callback_cb_ctr == 1

    # move to scenic (test2.kml only) then back to air — still a popup
    # because flight was in test2 region but NOT in any test1 region
    adsb_actions_two_kmls.loop((JSON_STRING_SCENIC+'\n')*3)
    adsb_actions_two_kmls.loop((JSON_STRING_AIR+'\n')*3)
    # takeoff_popup requires was in NO region at all, so this should NOT fire
    assert takeoff_callback_cb_ctr == 1

def test_kml_scoped_absence(adsb_actions_two_kmls):
    """~filename matches 'not in any region of that specific KML'."""
    global kml_scoped_cb_ctr
    kml_scoped_cb_ctr = 0

    # In scenic (test2.kml has a region, test1.kml does not) -> air (test1.kml)
    # kml_scoped_popup: was in ~test2.kml (no test2 region) AND now in Generic Gate Air
    # Should NOT fire because flight WAS in test2.kml Scenic region
    adsb_actions_two_kmls.loop((JSON_STRING_SCENIC+'\n')*3)
    adsb_actions_two_kmls.loop((JSON_STRING_AIR+'\n')*3)
    assert kml_scoped_cb_ctr == 0

    # From distant (no regions in either KML) -> air
    # Should fire because flight was NOT in any test2.kml region
    adsb_actions_two_kmls.loop((JSON_STRING_DISTANT+'\n')*3)
    adsb_actions_two_kmls.loop((JSON_STRING_AIR+'\n')*3)
    assert kml_scoped_cb_ctr == 1


YAML_JOINED_DEPARTED = """
  config:
    kmls:
      - tests/test1.kml

  rules:
    entered_air:
      conditions:
        joined_region: "Generic Gate Air"
      actions:
        callback: "joined_callback"

    left_air:
      conditions:
        departed_region: "Generic Gate Air"
      actions:
        callback: "departed_callback"
"""

@pytest.fixture
def adsb_actions_jd():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    testinfra.set_all_loggers(logging.DEBUG)
    Stats.reset()
    yaml_data = yaml.safe_load(YAML_JOINED_DEPARTED)
    aa = AdsbActions(yaml_data)
    aa.register_callback("joined_callback", joined_callback)
    aa.register_callback("departed_callback", departed_callback)
    yield aa

joined_cb_ctr = departed_cb_ctr = 0

def joined_callback(flight):
    global joined_cb_ctr
    joined_cb_ctr += 1

def departed_callback(flight):
    global departed_cb_ctr
    departed_cb_ctr += 1

def test_joined_departed(adsb_actions_jd):
    global joined_cb_ctr, departed_cb_ctr
    joined_cb_ctr = departed_cb_ctr = 0

    # Start outside all regions — no callbacks
    adsb_actions_jd.loop((JSON_STRING_DISTANT+'\n')*3)
    assert joined_cb_ctr == 0
    assert departed_cb_ctr == 0

    # Enter air region from no region — joined fires, departed does not
    adsb_actions_jd.loop((JSON_STRING_AIR+'\n')*3)
    assert joined_cb_ctr == 1
    assert departed_cb_ctr == 0

    # Stay in air region — no new callbacks
    adsb_actions_jd.loop((JSON_STRING_AIR+'\n')*3)
    assert joined_cb_ctr == 1
    assert departed_cb_ctr == 0

    # Move to ground region (leave air) — departed fires, joined does not
    adsb_actions_jd.loop((JSON_STRING_GROUND+'\n')*3)
    assert joined_cb_ctr == 1
    assert departed_cb_ctr == 1

    # Re-enter air from ground — joined fires again
    adsb_actions_jd.loop((JSON_STRING_AIR+'\n')*3)
    assert joined_cb_ctr == 2
    assert departed_cb_ctr == 1

    # Leave air to no region — departed fires again
    adsb_actions_jd.loop((JSON_STRING_DISTANT+'\n')*3)
    assert joined_cb_ctr == 2
    assert departed_cb_ctr == 2
