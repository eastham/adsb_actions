"Large test covering the 'regions' rule condition and 'note' action."

import logging

import yaml

from stats import Stats
import main
import rules
import testinfra

YAML_STRING = """
  config:
    kmls:
      - tests/test1.kml 

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

    distant_callback:
      conditions:
        regions: [ ~ ]
      actions:
        callback: "test_callback"
        """

JSON_STRING_DISTANT = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 41.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_GROUND = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_AIR = '{"now": 1661692178, "alt_baro": 4500, "gscp": 128, "lat": 40.748708, "lon": -119.2489313, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'
# different AC, some time in the future, should cause all current flights to expire:
JSON_STRING_GROUND_DELAY = '{"now": 1661692978, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345xxx"}\n'

def test_note():
    Stats.reset()
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    yaml_data = yaml.safe_load(YAML_STRING)
    f = main.setup_flights(yaml_data)
    r = rules.Rules(yaml_data)

    testinfra.process_adsb(JSON_STRING_DISTANT, f, r)
    testinfra.process_adsb(JSON_STRING_AIR, f, r)
    assert Stats.callbacks_fired == 2
    assert Stats.last_callback_flight
    assert Stats.last_callback_flight.flags['note'] == "saw_takeoff"

    testinfra.process_adsb(JSON_STRING_GROUND, f, r)
    assert Stats.callbacks_fired == 3
    assert Stats.last_callback_flight
    assert Stats.last_callback_flight.flags['note'] == "saw_takeoff"

    # cause expiration
    testinfra.process_adsb(JSON_STRING_GROUND_DELAY, f, r)

    # trigger another callback with these two
    testinfra.process_adsb(JSON_STRING_DISTANT, f, r)
    assert not 'note' in Stats.last_callback_flight.flags

    # XXX check note clearing behavior
