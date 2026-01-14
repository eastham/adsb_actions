"""Large test covering the 'regions' rule condition and 'note' action."""

import logging

import yaml

import testinfra
from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions

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


def test_resampler_interpolation_increases_points():
    """
    Test that the resampler increases the number of points when there are gaps between input points.
    """

    # Setup logging
    logging.basicConfig(format='%(levelname)s: %(message)s',
                        level=logging.DEBUG)
    logging.info('Starting interpolation growth test.')

    # Minimal YAML config for AdsbActions
    YAML_STRING = """
      config:
        kmls:
          - tests/test1.kml 
      rules: {}
    """

    # Three points, with a large time gap between the first and second
    ts_start = 1000
    ts_end_3 = 1040
    JSON_STRING_1 = '{"now": 1000, "alt_baro": 4000, "gscp": 128, "lat": 40.0, "lon": -119.0, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
    JSON_STRING_2 = '{"now": 1005, "alt_baro": 4100, "gscp": 128, "lat": 40.1, "lon": -119.1, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'
    JSON_STRING_3 = '{"now": 1040, "alt_baro": 4200, "gscp": 128, "lat": 40.2, "lon": -119.2, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'

    # and another, past the expire time, so it shouldn't be interpolated (100 second gap > 60 second EXPIRE_TIME)
    JSON_STRING_4 = '{"now": 1140, "alt_baro": 4200, "gscp": 128, "lat": 40.2, "lon": -119.2, "track": 203.4, "hex": "a061d9", "flight": "N12345"}\n'

    Stats.reset()
    yaml_data = yaml.safe_load(YAML_STRING)
    adsb_actions = AdsbActions(yaml_data, resample=True)
    testinfra.setup_test_callback(adsb_actions)

    # Feed the three points
    adsb_actions.loop(JSON_STRING_1)
    adsb_actions.loop(JSON_STRING_2)
    adsb_actions.loop(JSON_STRING_3)

    # Access the resampler
    resampler = adsb_actions.resampler

    # Now, count points after resampling/interpolation in tailhistory
    resampled_points = sum(len(locs)
                           for locs in resampler.locations_by_time.values())
    logging.info("Points in tailhistory after resampling: %d",
                 resampled_points)

    # The resampled/interpolated points should be greater than the original points
    assert resampled_points == ts_end_3 - ts_start + 1, (
        f"Expected more than 3 points after resampling, got {resampled_points}"
    )

    # Sanity checks: Verify actual interpolation is happening correctly
    # Check that we have data at specific timestamps
    assert 1000 in resampler.locations_by_time, "Missing timestamp 1000"
    assert 1020 in resampler.locations_by_time, "Missing timestamp 1020 (interpolated)"
    assert 1040 in resampler.locations_by_time, "Missing timestamp 1040"

    # Check midpoint between 1005 and 1040 (t=1022.5, approximately t=1022 or 1023)

    if 1023 in resampler.locations_by_time:
        loc_1023 = resampler.locations_by_time[1023][0]
        logging.info("Interpolated location at t=1023: lat=%.4f, lon=%.4f, alt=%d",
                     loc_1023.lat, loc_1023.lon, loc_1023.alt_baro)
        # Check that values are interpolated (not just copied from endpoints)
        assert 40.1 < loc_1023.lat < 40.2, f"Expected lat between 40.1 and 40.2, got {loc_1023.lat}"
        assert -119.2 < loc_1023.lon < -119.1, f"Expected lon between -119.2 and -119.1, got {loc_1023.lon}"
        assert 4100 < loc_1023.alt_baro < 4200, f"Expected alt between 4100 and 4200, got {loc_1023.alt_baro}"
    else:
        assert False, "Missing interpolated timestamp 1023"

    # Check that we have continuous timestamps (no gaps in interpolation)
    timestamps = sorted(resampler.locations_by_time.keys())
    for i in range(len(timestamps) - 1):
        if timestamps[i] < 1040:  # Only check before the gap to timestamp 1100
            time_gap = timestamps[i + 1] - timestamps[i]
            assert time_gap == 1, f"Found gap of {time_gap} seconds between {timestamps[i]} and {timestamps[i+1]}"

    logging.info("Interpolation sanity checks passed!")

    adsb_actions.loop(JSON_STRING_4)
    resampled_points_after4 = sum(len(locs)
                                  for locs in resampler.locations_by_time.values())
    logging.info("Points in locations_by_time after second resampling: %d",
                 resampled_points_after4)
    
    # After the second loop, we should have only one more point due to
    # long time gap
    assert resampled_points_after4 == resampled_points + 1, (
        f"Expected resampling growth of only 1 point, got {resampled_points_after4 - resampled_points}"
    )