import logging
from io import StringIO
import yaml
from stats import Stats
import main

YAML_STRING = """
  config:
    kmls:
      - /Users/eastham/brc-charts/88nvnewgates4.kml 

  aircraft_lists:
    banned: [ "N12345" ] 

  rules:
    banned_aircraft:
      conditions:
        aircraft_list: banned
        cooldown: 180 # minutes
      actions:
        callback: empty_callback
"""

JSON_STRING = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
JSON_STRING_100S_LATER = '{"now": 1661692278, "alt_baro": 4500, "gscp": 128, "lat": 40.748708, "lon": -119.2489313, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'
JSON_STRING_20000S_LATER = '{"now": 1661712278, "alt_baro": 4500, "gscp": 128, "lat": 40.748708, "lon": -119.2489313, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'

def run_workload(yaml_data, input_str):
    adsb_test_buf = StringIO(input_str)
    listen = main.TCPConnection()
    listen.f = adsb_test_buf

    main.start(yaml_data, listen)

def test_cooldown():
    Stats.reset()

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    yaml_data = yaml.safe_load(YAML_STRING)

    run_workload(yaml_data, JSON_STRING)
    assert Stats.callbacks_fired == 1

    run_workload(yaml_data, JSON_STRING_100S_LATER)
    assert Stats.callbacks_fired == 1 # no new callbacks due to cooldown

    run_workload(yaml_data, JSON_STRING_20000S_LATER)
    assert Stats.callbacks_fired == 2 




