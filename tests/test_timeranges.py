"""Test of the 'time_ranges' condition."""
import logging
import yaml
import testinfra
from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions

YAML_STRING = """
  config:
    kmls:
      - tests/test1.kml 

  rules:
    after_hours_aircraft:
      conditions:
        time_ranges: [ "0000-0130", "1400-2400" ]
      actions:
        callback: test_callback
"""

# 01:00 UTC (should match first range)
JSON_STRING_0100 = '{"now": 1754355600, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N54321"}\n'
# 02:00 UTC (should NOT match any range)
JSON_STRING_0200 = '{"now": 1754359200, "alt_baro": 4100, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N54321"}\n'
# 14:00 UTC (should match second range)
JSON_STRING_1400 = '{"now": 1754402400, "alt_baro": 4200, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N54321"}\n'


def test_time_ranges():
    Stats.reset()
    logging.basicConfig(format='%(levelname)s: %(message)s',
                        level=logging.DEBUG)
    logging.info('Testing time_ranges condition.')

    yaml_data = yaml.safe_load(YAML_STRING)
    adsb_actions = AdsbActions(yaml_data)
    testinfra.setup_test_callback(adsb_actions)

    # 01:00 UTC - should fire
    adsb_actions.loop(JSON_STRING_0100)
    assert Stats.callbacks_fired == 1

    # 02:00 UTC - should NOT fire (outside any range)
    adsb_actions.loop(JSON_STRING_0200)
    assert Stats.callbacks_fired == 1

    # 14:00 UTC - should fire (exact match on second range)
    adsb_actions.loop(JSON_STRING_1400)
    assert Stats.callbacks_fired == 2


def test_time_ranges_wraparound():
    """
    Test that the time_ranges condition works for ranges that wrap around midnight.
    """
    YAML_STRING_WRAP = """
      config:
        kmls:
          - tests/test1.kml 

      rules:
        night_aircraft:
          conditions:
            time_ranges: [ "2200-0200" ]
          actions:
            callback: test_callback
    """

    # 23:00 UTC (should match, inside wraparound range)
    JSON_STRING_2300 = '{"now": 1754434800, "alt_baro": 4000, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N54321"}\n'
    # 01:00 UTC (should match, inside wraparound range)
    JSON_STRING_0100 = '{"now": 1754355600, "alt_baro": 4100, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N54321"}\n'
    # 03:00 UTC (should NOT match, outside wraparound range)
    JSON_STRING_0300 = '{"now": 1754362800, "alt_baro": 4200, "gscp": 128, "lat": 40.763537, "lon": -119.2122323, "track": 203.4, "hex": "a061d9", "flight": "N54321"}\n'

    Stats.reset()
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)
    logging.info('Testing wraparound time_ranges condition.')

    yaml_data = yaml.safe_load(YAML_STRING_WRAP)
    adsb_actions = AdsbActions(yaml_data)
    testinfra.setup_test_callback(adsb_actions)

    # 23:00 UTC - should fire
    adsb_actions.loop(JSON_STRING_2300)
    assert Stats.callbacks_fired == 1

    # 01:00 UTC - should fire (still in wraparound range)
    adsb_actions.loop(JSON_STRING_0100)
    assert Stats.callbacks_fired == 2

    # 03:00 UTC - should NOT fire (outside wraparound range)
    adsb_actions.loop(JSON_STRING_0300)
    assert Stats.callbacks_fired == 2
