"""
Huge integration test that takes an hour of busy airspace data 
and checks that we're detecting the right number of takeoffs/landings/local flights.
"""

import logging

import yaml

from stats import Stats
from adsbactions import AdsbActions
import testinfra

YAML_STRING= """
  config:
    kmls:
      - tests/test1.kml 

  rules:
    takeoff:
      conditions:
        transition_regions: [ "Generic Gate Ground", "Generic Gate Air" ]
      actions:
        callback: "takeoff"
        note: "saw_takeoff"

    landing:
      conditions:
        transition_regions: [ "Generic Gate Air", "Generic Gate Ground" ]
      actions:
        callback: "landing"
"""

landing_ctr = local_landing_ctr = 0
takeoff_ctr = 0

def landing_cb(flight):
    global landing_ctr, local_landing_ctr
    landing_ctr += 1
    if 'note' in flight.flags:
        logging.info("Local-flight landing detected!")
        local_landing_ctr += 1

def takeoff_cb(flight):
    global takeoff_ctr
    takeoff_ctr += 1

def test_1hr():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.WARNING)

    # turn down loggers systemwide for noise/perf reasons
    testinfra.set_all_loggers(logging.INFO)

    Stats.reset()
    yaml_data = yaml.safe_load(YAML_STRING)

    with open("tests/1hr.json", 'rt', encoding="utf-8") as myfile:
        json_data = myfile.read()

    adsb_actions = AdsbActions(yaml_data)
    adsb_actions.register_callback("landing", landing_cb)
    adsb_actions.register_callback("takeoff", takeoff_cb)
    adsb_actions.loop(json_data)

    assert takeoff_ctr == 14
    assert landing_ctr == 18
    assert local_landing_ctr == 9
