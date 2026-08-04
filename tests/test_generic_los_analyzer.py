"""
Clean demo test for generic_los_analyzer.py

This test demonstrates the LOS analyzer with a simple scenario:
Two aircraft fly through the 10nm ring around San Carlos Airport (KSQL)
at different times, then converge and violate separation minimums.
"""
import logging
import yaml

from adsb_actions.adsbactions import AdsbActions

# Counter for detected LOS events
los_event_count = 0


def los_event_cb(flight1, flight2):
    """Callback that counts LOS events."""
    global los_event_count
    los_event_count += 1

    # Calculate separation for logging
    lat_sep = flight1.lastloc - flight2.lastloc
    alt_sep = abs(flight1.lastloc.alt_baro - flight2.lastloc.alt_baro)

    logging.warning(
        "!!! LOS DETECTED: %s and %s - Separation: %.2f nm, %d ft",
        flight1.flight_id,
        flight2.flight_id,
        lat_sep,
        alt_sep
    )


# YAML configuration matching generic_los_rules.yaml
# Simple configuration without geographic filtering
# The test uses pedantic=True mode to check all aircraft
# Altitude: 3000-5000 ft MSL (must be within resampler range of 3000-12000 ft)
# Separation: 400 ft vertical, 0.3 nm lateral
YAML_CONFIG = """
rules:
  los_detection:
    conditions:
      min_alt: 3000
      max_alt: 5000
      proximity: [400, 0.3]
    actions:
      callback: los_event_cb
"""

# Test aircraft data - two aircraft crossing paths near San Carlos Airport (KSQL)
# Note: Altitudes must be 3000-12000 ft for resampler to work (optimization in resampler.py)
# Aircraft 1: Flying north-south through area (varying latitude only)
AIRCRAFT1_POSITION1 = '{"now": 1661692168, "alt_baro": 4000, "gscp": 128, "lat": 37.45, "lon": -122.1336099, "track": 0, "hex": "abc001"}\n'
AIRCRAFT1_POSITION2 = '{"now": 1661692188, "alt_baro": 4000, "gscp": 128, "lat": 37.47, "lon": -122.1336099, "track": 0, "hex": "abc001"}\n'

# Aircraft 2: Flying west-east through area (varying longitude only, crosses Aircraft 1)
AIRCRAFT2_POSITION1 = '{"now": 1661692168, "alt_baro": 4000, "gscp": 128, "lat": 37.4612846, "lon": -122.15, "track": 90, "hex": "abc002"}\n'
AIRCRAFT2_POSITION2 = '{"now": 1661692188, "alt_baro": 4000, "gscp": 128, "lat": 37.4612846, "lon": -122.12, "track": 90, "hex": "abc002"}\n'


def test_los_detection_demo():
    """
    Single clean test demonstrating LOS detection.

    Scenario:
    - Two aircraft enter a 10nm ring around San Carlos Airport (KSQL)
    - Both are between 1000-3000 ft MSL
    - They converge to within 0.3 nm laterally and 100 ft vertically
    - The resampler interpolates their positions and detects the LOS event

    Expected: At least 1 LOS event detected during convergence
    """
    global los_event_count
    los_event_count = 0

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    logging.info('=' * 70)
    logging.info('Generic LOS Analyzer Demo Test')
    logging.info('=' * 70)
    logging.info('Scenario: Two aircraft cross paths at same altitude')
    logging.info('Altitude range: 3000-5000 ft MSL (no geographic filtering)')
    logging.info('Separation minimums: 400 ft vertical, 0.3 nm lateral')
    logging.info('-' * 70)

    # Load configuration
    yaml_data = yaml.safe_load(YAML_CONFIG)

    # Create AdsbActions with resampling enabled
    # Use pedantic=True to check all aircraft (not just those in geographic regions)
    adsb_actions = AdsbActions(yaml_data, resample=True, pedantic=True)
    adsb_actions.register_callback("los_event_cb", los_event_cb)

    # Phase 1: Collect position data
    logging.info('Phase 1: Collecting aircraft position data...')
    adsb_actions.loop(AIRCRAFT1_POSITION1)
    adsb_actions.loop(AIRCRAFT1_POSITION2)
    adsb_actions.loop(AIRCRAFT2_POSITION1)
    adsb_actions.loop(AIRCRAFT2_POSITION2)
    logging.info('  Aircraft 1: lat 37.45 → 37.47, lon -122.134 @ 4000 ft (north-south)')
    logging.info('  Aircraft 2: lat 37.461, lon -122.15 → -122.12 @ 4000 ft (west-east)')
    logging.info('  Paths cross at: 37.4612846, -122.1336099')

    # Phase 2: Run resampled proximity checks
    logging.info('Phase 2: Running resampled proximity checks...')
    prox_events = adsb_actions.do_resampled_prox_checks(gc_callback=None)

    # Results
    logging.info('-' * 70)
    logging.info('Results:')
    logging.info('  LOS events detected: %d', los_event_count)
    logging.info('  Proximity events returned: %d', len(prox_events) if prox_events else 0)
    logging.info('=' * 70)

    # Verify LOS was detected
    assert los_event_count > 0, "Expected at least one LOS event to be detected"
    logging.info('✓ Test passed: LOS detection working correctly')
