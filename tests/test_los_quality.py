"""Unit tests for LOS event quality scoring."""

from unittest.mock import Mock
from src.applications.airport_monitor.los import calculate_event_quality


def _make_flight(flight_id, first_now, last_now, category='A1'):
    """Helper to build a flight mock with required string flight_id."""
    f = Mock()
    f.flight_id = flight_id
    f.firstloc = Mock(now=first_now)
    f.lastloc = Mock(now=last_now, flightdict={'category': category})
    return f


class TestEventQuality:
    """Test the event quality calculation logic."""

    def test_low_quality_long_duration(self):
        """Event duration > 120 seconds should be low quality."""
        los = Mock()
        los.create_time = 100.0
        los.last_time = 250.0  # 150 second duration (> 120)
        los.min_latdist = 0.5  # nm
        los.min_altdist = 500  # ft

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'low'
        assert 'formation' in explanation.lower()

    def test_low_quality_short_track(self):
        """Track duration < 60 seconds should be low quality."""
        los = Mock()
        los.create_time = 100.0
        los.last_time = 145.0  # 45 second event duration
        los.min_latdist = 0.5  # nm
        los.min_altdist = 500  # ft

        flight1 = _make_flight('N123AB', 50.0, 90.0)   # 40 sec track (< 60)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'low'
        assert 'short track' in explanation.lower() or 'insufficient data' in explanation.lower()

    def test_medium_quality_moderate_duration(self):
        """Event duration 60-120 seconds should be medium quality."""
        los = Mock()
        los.create_time = 100.0
        los.last_time = 190.0  # 90 second duration (60 < x <= 120)
        los.min_latdist = 0.5  # nm
        los.min_altdist = 500  # ft

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'medium'
        assert 'moderate duration' in explanation.lower()

    def test_medium_quality_helicopter(self):
        """Helicopter involvement (category A7) should be medium quality."""
        los = Mock()
        los.create_time = 100.0
        los.last_time = 130.0  # 30 second duration
        los.min_latdist = 0.5  # nm
        los.min_altdist = 500  # ft

        flight1 = _make_flight('N123AB', 50.0, 300.0, category='A7')
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'medium'
        assert 'helicopter' in explanation.lower()

    def test_medium_quality_helicopter_flight2(self):
        """Helicopter as flight2 should also be medium quality."""
        los = Mock()
        los.create_time = 100.0
        los.last_time = 130.0  # 30 second duration
        los.min_latdist = 0.5  # nm
        los.min_altdist = 500  # ft

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0, category='A7')

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'medium'
        assert 'helicopter' in explanation.lower()

    def test_high_quality(self):
        """Short duration (<= 40s), long tracks, no helicopters should be high quality."""
        los = Mock()
        los.create_time = 100.0
        los.last_time = 130.0  # 30 second duration (<= 40)
        los.min_latdist = 0.5  # nm (> 0.2, so not vhigh)
        los.min_altdist = 500  # ft (> 200, so not vhigh)

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'high'
        assert 'brief' in explanation.lower() or 'good' in explanation.lower()

    def test_boundary_duration_2min(self):
        """Event duration exactly 120 seconds is NOT low quality (> not >=)."""
        los = Mock()
        los.create_time = 100.0
        los.last_time = 220.0  # Exactly 120 second duration
        los.min_latdist = 0.5  # nm
        los.min_altdist = 500  # ft

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        # 120 seconds should be medium (60 < 120 <= 120), not low (> 120)
        quality, _ = calculate_event_quality(los, flight1, flight2)
        assert quality == 'medium'

    def test_boundary_duration_40sec(self):
        """Event duration exactly 40 seconds is NOT medium (> not >=)."""
        los = Mock()
        los.create_time = 100.0
        los.last_time = 140.0  # Exactly 40 second duration
        los.min_latdist = 0.5  # nm
        los.min_altdist = 500  # ft

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        # 40 seconds should be high (<=40), not medium (>40)
        quality, _ = calculate_event_quality(los, flight1, flight2)
        assert quality == 'high'

    def test_boundary_track_60sec(self):
        """Track duration exactly 60 seconds is NOT low (< not <=)."""
        los = Mock()
        los.create_time = 100.0
        los.last_time = 130.0  # 30 second event
        los.min_latdist = 0.5  # nm
        los.min_altdist = 500  # ft

        flight1 = _make_flight('N123AB', 50.0, 110.0)  # Exactly 60 sec track
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        # 60 seconds track should be high, not low (<60)
        quality, _ = calculate_event_quality(los, flight1, flight2)
        assert quality == 'high'

    def test_vhigh_quality_close_cpa(self):
        """High quality event with very close CPA should be vhigh."""
        los = Mock()
        los.create_time = 100.0
        los.last_time = 130.0  # 30 second duration (<= 40)
        los.min_latdist = 0.15  # nm (< 0.2)
        los.min_altdist = 150  # ft (< 200)

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'vhigh'
        assert 'close cpa' in explanation.lower()
        assert '0.15nm' in explanation or '150ft' in explanation

    def test_vhigh_boundary_lateral(self):
        """CPA exactly 0.2 nm should NOT be vhigh (< not <=)."""
        los = Mock()
        los.create_time = 100.0
        los.last_time = 130.0  # 30 second duration (<= 40)
        los.min_latdist = 0.2  # nm (exactly 0.2, not < 0.2)
        los.min_altdist = 100  # ft (< 200)

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, _ = calculate_event_quality(los, flight1, flight2)
        assert quality == 'high'  # Not vhigh

    def test_vhigh_boundary_vertical(self):
        """CPA exactly 200 ft should NOT be vhigh (< not <=)."""
        los = Mock()
        los.create_time = 100.0
        los.last_time = 130.0  # 30 second duration (<= 40)
        los.min_latdist = 0.1  # nm (< 0.2)
        los.min_altdist = 200  # ft (exactly 200, not < 200)

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, _ = calculate_event_quality(los, flight1, flight2)
        assert quality == 'high'  # Not vhigh

    def test_vhigh_requires_both_cpa_criteria(self):
        """vhigh requires BOTH lateral < 0.2nm AND vertical < 200ft."""
        los1 = Mock()
        los1.create_time = 100.0
        los1.last_time = 130.0
        los1.min_latdist = 0.1  # nm (< 0.2)
        los1.min_altdist = 300  # ft (> 200) - fails vertical

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, _ = calculate_event_quality(los1, flight1, flight2)
        assert quality == 'high'  # Not vhigh - vertical too large

        los2 = Mock()
        los2.create_time = 100.0
        los2.last_time = 130.0
        los2.min_latdist = 0.3  # nm (> 0.2) - fails lateral
        los2.min_altdist = 100  # ft (< 200)

        quality, _ = calculate_event_quality(los2, flight1, flight2)
        assert quality == 'high'  # Not vhigh - lateral too large

    def test_vhigh_negative_altitude_difference(self):
        """vhigh should handle negative altitude differences (use absolute value)."""
        los = Mock()
        los.create_time = 100.0
        los.last_time = 130.0
        los.min_latdist = 0.15  # nm (< 0.2)
        los.min_altdist = -150  # ft (negative, but abs < 200)

        flight1 = _make_flight('N123AB', 50.0, 300.0)
        flight2 = _make_flight('N456CD', 50.0, 300.0)

        quality, explanation = calculate_event_quality(los, flight1, flight2)
        assert quality == 'vhigh'
        assert 'close cpa' in explanation.lower()
