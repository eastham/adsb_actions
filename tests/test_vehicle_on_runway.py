"""Tests vehicle-on-runway detection with cooldown: a tracked ground vehicle enters a runway region,
with entries spaced to verify the 30s cooldown fires or suppresses correctly."""

import logging
import yaml
import pytest

import testinfra
from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions

YAML_STRING = """
  config:
    kmls:
      - examples/88nv/regions/runways.kml

  aircraft_lists:
    ground_vehicles: ["N10DA"]

  rules:
    vehicle_on_runway:
      conditions:
        regions: [ "23r ground", "23L ground" ]
        aircraft_list: ground_vehicles
        cooldown: 0.5
      actions:
        callback: "vehicle_on_runway_cb"
"""

# Four entries inside a runway region, each 32s apart (clears the 30s cooldown).
# A fifth entry is only 15s after the fourth and must NOT fire.
# Coordinates: (40.76870, -119.18520) is inside "23L ground".
_T0 = 1693383000
_IN  = '{"now": %d, "alt_baro": 4000, "gscp": 5, "lat": 40.76870, "lon": -119.18520, "track": 50.0, "hex": "adf815", "flight": "TRUCK1"}'
_OUT = '{"now": %d, "alt_baro": 4000, "gscp": 5, "lat": 41.00000, "lon": -119.20000, "track": 50.0, "hex": "adf815", "flight": "TRUCK1"}'

TRUCK_DATA = "\n".join([
    _IN  % (_T0 + 0),    # t=0:   enters runway -> fires
    _OUT % (_T0 + 1),    # t=1:   exits runway
    _IN  % (_T0 + 32),   # t=32:  re-enters, 32s since last fire -> fires
    _OUT % (_T0 + 33),   # t=33:  exits runway
    _IN  % (_T0 + 64),   # t=64:  re-enters, 32s since last fire -> fires
    _OUT % (_T0 + 65),   # t=65:  exits runway
    _IN  % (_T0 + 96),   # t=96:  re-enters, 32s since last fire -> fires
    _OUT % (_T0 + 97),   # t=97:  exits runway
    _IN  % (_T0 + 111),  # t=111: re-enters only 15s later -> suppressed by cooldown
]) + "\n"

vehicle_on_runway_cb_ctr = 0

def vehicle_on_runway_cb(flight):
    global vehicle_on_runway_cb_ctr
    vehicle_on_runway_cb_ctr += 1

@pytest.fixture
def adsb_actions():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    testinfra.set_all_loggers(logging.DEBUG)
    Stats.reset()
    yaml_data = yaml.safe_load(YAML_STRING)
    aa = AdsbActions(yaml_data)
    aa.register_callback("vehicle_on_runway_cb", vehicle_on_runway_cb)
    yield aa

def test_vehicle_on_runway_cooldown(adsb_actions):
    global vehicle_on_runway_cb_ctr
    vehicle_on_runway_cb_ctr = 0

    adsb_actions.loop(string_data=TRUCK_DATA)

    # 4 entries clear the 30s cooldown; 5th entry is only 15s after the 4th.
    assert vehicle_on_runway_cb_ctr == 4
    assert Stats.callbacks_fired == 4
