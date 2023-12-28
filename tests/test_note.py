"Large test covering the 'regions' rule condition and 'note' action."

import logging
from io import StringIO
import yaml
from stats import Stats
import main

YAML_STRING = """
  config:
    kmls:
      - /Users/eastham/brc-charts/88nvnewgates4.kml 

  aircraft_lists:  # this is probably not the right way to do this
    banned: [ "N42PE", "N12345" ] 

  rules:
    takeoff_popup:
      conditions:
        transition_regions: [ ~, "Generic Gate Air" ]
      actions:
        callback: "test_callback"
        note: "saw_takeoff" # tested here

    landing:
      conditions:
        transition_regions: [ "Generic Gate Air", "Generic Gate Ground" ]
        regions: [ "Generic Gate Ground" ]
      actions:
        callback: "test_callback"  # note is contained in flight note

    should_never_match:
      conditions:
        regions: [ "Generic Gate XXX" ]
      actions:
        callback: "test_callback"
        """

JSON_STRING_DISTANT = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 41.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_GROUND = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_AIR = '{"now": 1661692178, "alt_baro": 4500, "gscp": 128, "lat": 40.748708, "lon": -119.2489313, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'

def run_workload(yaml_data, input_str):
    adsb_test_buf = StringIO(input_str)
    listen = main.TCPConnection()
    listen.f = adsb_test_buf

    main.start(yaml_data, listen)

def test_transitions():
    Stats.reset()
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    yaml_data = yaml.safe_load(YAML_STRING)

    run_workload(yaml_data, JSON_STRING_DISTANT)
    run_workload(yaml_data, JSON_STRING_AIR)
    assert Stats.callbacks_fired == 1
    assert Stats.last_callback_flight
    assert Stats.last_callback_flight.flags['note'] == "saw_takeoff"

    run_workload(yaml_data, JSON_STRING_GROUND)
    assert Stats.callbacks_fired == 2
    assert Stats.last_callback_flight
    assert Stats.last_callback_flight.flags['note'] == "saw_takeoff"
