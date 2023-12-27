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
    banned_slack:
      conditions:
        aircraft_list: banned
      actions:
        slack: True
        page: True

    takeoff:
      conditions:
        transition_regions: [ "Generic Gate Ground", "Generic Gate Air" ]
      actions:
        callback: "add_op"
        note: "saw_takeoff"
"""

JSON_STRING1 = '{"now": 1661692178, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'
JSON_STRING2 = '{"now": 1661692178, "alt_baro": 4500, "gscp": 128, "lat": 40.748708, "lon": -119.2489313, "track": 203.4, "hex": "a061d9", "flight": "N12345"}'

def test_main():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('System started.')

    yaml_data = yaml.safe_load(YAML_STRING)

    adsb_test_buf = StringIO(JSON_STRING1)
    listen = main.TCPConnection()
    listen.f = adsb_test_buf

    Stats.reset()
    assert Stats.json_readlines == 0

    main.start(yaml_data, listen)
    assert Stats.json_readlines == 1
    assert Stats.condition_match_calls == 2
    assert Stats.condition_matches_true == 1
    assert Stats.callbacks_fired == 0

    adsb_test_buf = StringIO(JSON_STRING2)
    listen = main.TCPConnection()
    listen.f = adsb_test_buf
    main.start(yaml_data, listen)

    assert Stats.json_readlines == 2
    assert Stats.condition_match_calls == 4  # 2 per position
    assert Stats.condition_matches_true == 3  # 1 the first positon, 2 second
    assert Stats.callbacks_fired == 1