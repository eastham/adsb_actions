"""Unit tests for LOS event quality scoring."""

from unittest.mock import Mock
from adsb_actions.location import Location
from src.applications.airport_monitor.los import calculate_event_quality


def _make_flight(flight_id, first_now, last_now, category='A1', suspicious=False):
    """Helper to build a flight mock with required string flight_id."""
    f = Mock()
    f.flight_id = flight_id
    f.firstloc = Mock(now=first_now)
    f.lastloc = Mock(now=last_now, flightdict={'category': category})
    f.flags = {'suspicious': True} if suspicious else {}
    return f


def _make_los(create_time, last_time, min_latdist=0.5, min_altdist=500,
              cpa_time=None, event_locations=None):
    """Helper to build a los mock. event_locations defaults to empty dict
    (no location data, i.e. streaming mode)."""
    los = Mock()
    los.create_time = create_time
    los.last_time = last_time
    los.min_latdist = min_latdist
    los.min_altdist = min_altdist
    los.cpa_time = cpa_time if cpa_time is not None else create_time
    los.event_locations = event_locations if event_locations is not None else {}
    return los


def _make_locations(flight_id, start, end, interval=1):
    """Create a list of real Location objects at regular intervals."""
    return [Location(lat=37.0, lon=-122.0, alt_baro=2000,
                     now=start + i * interval, flight=flight_id)
            for i in range(int((end - start) / interval) + 1)]


class TestEventQuality:
    """Test the event quality calculation logic."""

    def test_low_quality_long_duration(self):
        """Event duration > 120 seconds should be low quality."""
        los = _make_los(100.0, 250.0)  # 150 second duration (> 120)

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'low'
        assert 'formation' in explanation.lower()

    def test_low_quality_short_track(self):
        """Track duration < 60 seconds should be low quality."""
        los = _make_los(100.0, 145.0)  # 45 second event duration

        flight1 = _make_flight('N123AB', 50.0, 90.0)   # 40 sec track (< 60)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'low'
        assert 'short track' in explanation.lower() or 'insufficient data' in explanation.lower()

    def test_medium_quality_moderate_duration(self):
        """Event duration 60-120 seconds should be medium quality."""
        los = _make_los(100.0, 190.0)  # 90 second duration

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'medium'
        assert 'moderate duration' in explanation.lower()

    def test_medium_quality_helicopter(self):
        """Helicopter involvement (category A7) should be medium quality."""
        los = _make_los(100.0, 130.0)  # 30 second duration

        flight1 = _make_flight('N123AB', 50.0, 300.0, category='A7')
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'medium'
        assert 'helicopter' in explanation.lower()

    def test_medium_quality_helicopter_flight2(self):
        """Helicopter as flight2 should also be medium quality."""
        los = _make_los(100.0, 130.0)

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0, category='A7')

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'medium'
        assert 'helicopter' in explanation.lower()

    def test_high_quality(self):
        """Short duration (<= 40s), long tracks, no helicopters should be high quality."""
        los = _make_los(100.0, 130.0)  # 30 second duration

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'high'
        assert 'brief' in explanation.lower() or 'good' in explanation.lower()

    def test_boundary_duration_2min(self):
        """Event duration exactly 120 seconds is NOT low quality (> not >=)."""
        los = _make_los(100.0, 220.0)  # Exactly 120 second duration

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, _ = calculate_event_quality(los, flight1, flight2)
        assert quality == 'medium'

    def test_boundary_duration_40sec(self):
        """Event duration exactly 40 seconds is NOT medium (> not >=)."""
        los = _make_los(100.0, 140.0)  # Exactly 40 second duration

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, _ = calculate_event_quality(los, flight1, flight2)
        assert quality == 'high'

    def test_boundary_track_60sec(self):
        """Track duration exactly 60 seconds is NOT low (< not <=)."""
        los = _make_los(100.0, 130.0)  # 30 second event

        flight1 = _make_flight('N123AB', 50.0, 110.0)  # Exactly 60 sec track
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, _ = calculate_event_quality(los, flight1, flight2)
        assert quality == 'high'

    def test_vhigh_quality_close_cpa(self):
        """High quality event with very close CPA should be vhigh."""
        los = _make_los(100.0, 130.0, min_latdist=0.15, min_altdist=150)

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'vhigh'
        assert 'close cpa' in explanation.lower()
        assert '0.15nm' in explanation or '150ft' in explanation

    def test_vhigh_boundary_lateral(self):
        """CPA exactly 0.2 nm should NOT be vhigh (< not <=)."""
        los = _make_los(100.0, 130.0, min_latdist=0.2, min_altdist=100)

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, _ = calculate_event_quality(los, flight1, flight2)
        assert quality == 'high'

    def test_vhigh_boundary_vertical(self):
        """CPA exactly 200 ft should NOT be vhigh (< not <=)."""
        los = _make_los(100.0, 130.0, min_latdist=0.1, min_altdist=200)

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, _ = calculate_event_quality(los, flight1, flight2)
        assert quality == 'high'

    def test_vhigh_requires_both_cpa_criteria(self):
        """vhigh requires BOTH lateral < 0.2nm AND vertical < 200ft."""
        los1 = _make_los(100.0, 130.0, min_latdist=0.1, min_altdist=300)

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, _ = calculate_event_quality(los1, flight1, flight2)
        assert quality == 'high'  # Not vhigh - vertical too large

        los2 = _make_los(100.0, 130.0, min_latdist=0.3, min_altdist=100)

        quality, _ = calculate_event_quality(los2, flight1, flight2)
        assert quality == 'high'  # Not vhigh - lateral too large

    def test_vhigh_negative_altitude_difference(self):
        """vhigh should handle negative altitude differences (use absolute value)."""
        los = _make_los(100.0, 130.0, min_latdist=0.15, min_altdist=-150)

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'vhigh'
        assert 'close cpa' in explanation.lower()


class TestResampledDataQuality:
    """Test location-level quality heuristics using event_locations."""

    def test_no_real_data_near_cpa_low_quality(self):
        """No real reports within ±10s of CPA → low quality."""
        # CPA at t=115, but real reports only at t=100-103 (far from CPA)
        locs1 = _make_locations('N123AB_1', 100, 103)
        locs2 = _make_locations('N456CD_1', 100, 103)

        los = _make_los(100.0, 130.0, cpa_time=115.0,
                        event_locations={'N123AB_1': locs1, 'N456CD_1': locs2})

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'low'
        assert 'no real data near cpa' in explanation.lower()

    def test_no_real_data_near_cpa_one_aircraft(self):
        """No real reports near CPA for just one aircraft → low quality."""
        # Flight 1 has good data, flight 2 has gap near CPA
        locs1 = _make_locations('N123AB_1', 100, 130)  # every second
        locs2 = _make_locations('N456CD_1', 100, 103)   # only far from CPA at 115

        los = _make_los(100.0, 130.0, cpa_time=115.0,
                        event_locations={'N123AB_1': locs1, 'N456CD_1': locs2})

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'low'
        assert 'N456CD_1' in explanation

    def test_sparse_data_caps_at_medium(self):
        """<75% real reports during LOS → high/vhigh capped to medium."""
        # 30s LOS event, but only ~5 real reports (way below 75%)
        locs1 = _make_locations('N123AB_1', 100, 130, interval=6)  # ~5 reports
        locs2 = _make_locations('N456CD_1', 100, 130, interval=6)

        los = _make_los(100.0, 130.0, cpa_time=115.0,
                        event_locations={'N123AB_1': locs1, 'N456CD_1': locs2})

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'medium'
        assert 'sparse data' in explanation.lower()

    def test_sparse_data_does_not_affect_already_low(self):
        """Sparse data heuristic should not override already-low quality."""
        # Long event (>120s) → already low; sparse data doesn't change that
        locs1 = _make_locations('N123AB_1', 100, 260, interval=10)
        locs2 = _make_locations('N456CD_1', 100, 260, interval=10)

        los = _make_los(100.0, 260.0, cpa_time=180.0,  # 160s duration → low
                        event_locations={'N123AB_1': locs1, 'N456CD_1': locs2})

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, _ = calculate_event_quality(los, flight1, flight2)
        assert quality == 'low'

    def test_good_data_no_downgrade(self):
        """Dense real reports during LOS → quality unchanged."""
        # Every second for both flights throughout the LOS
        locs1 = _make_locations('N123AB_1', 100, 130)
        locs2 = _make_locations('N456CD_1', 100, 130)

        los = _make_los(100.0, 130.0, cpa_time=115.0,
                        event_locations={'N123AB_1': locs1, 'N456CD_1': locs2})

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'high'
        assert 'sparse' not in explanation.lower()
        assert 'no real' not in explanation.lower()

    def test_empty_event_locations_no_downgrade(self):
        """Empty event_locations (streaming mode) → no location-based downgrades."""
        los = _make_los(100.0, 130.0)  # default: event_locations = {}

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, _ = calculate_event_quality(los, flight1, flight2)
        assert quality == 'high'

    def test_vhigh_downgraded_to_medium_by_sparse_data(self):
        """vhigh event with sparse data should be capped at medium."""
        # Would be vhigh (close CPA) but sparse data
        locs1 = _make_locations('N123AB_1', 100, 130, interval=6)
        locs2 = _make_locations('N456CD_1', 100, 130, interval=6)

        los = _make_los(100.0, 130.0, min_latdist=0.15, min_altdist=150,
                        cpa_time=115.0,
                        event_locations={'N123AB_1': locs1, 'N456CD_1': locs2})

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'medium'
        assert 'sparse data' in explanation.lower()
