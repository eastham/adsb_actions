"""Test quality explanation generation."""

from unittest.mock import Mock
from src.applications.airport_monitor.los import calculate_event_quality


def _make_flight(flight_id, first_now, last_now, category='A1'):
    """Helper to build a flight mock with required string flight_id."""
    f = Mock()
    f.flight_id = flight_id
    f.firstloc = Mock(now=first_now)
    f.lastloc = Mock(now=last_now, flightdict={'category': category})
    f.flags = {}
    return f


def _make_los(create_time, last_time, min_latdist=0.5, min_altdist=500):
    los = Mock()
    los.create_time = create_time
    los.last_time = last_time
    los.min_latdist = min_latdist
    los.min_altdist = min_altdist
    los.location_history = {}  # empty = streaming mode, no location-based downgrades
    return los


def test_quality_explanations():
    """Demonstrate the various quality explanations."""

    # Test 1: Long event (> 60s without location data to disprove formation) → low
    los = _make_los(100.0, 250.0)  # 150 seconds

    flight1 = _make_flight('N123AB', 50.0, 300.0)
    flight2 = _make_flight('N456CD', 50.0, 300.0)

    quality, explanation = calculate_event_quality(los, flight1, flight2)
    print(f"\nLong event: quality={quality}, explanation={explanation}")
    assert quality == 'low'
    assert 'formation' in explanation.lower()

    # Test 2: Short track (insufficient data) → low
    los = _make_los(100.0, 145.0)  # 45 seconds

    flight1 = _make_flight('N123AB', 90.0, 120.0)  # 30 second track
    flight2 = _make_flight('N456CD', 50.0, 300.0)

    quality, explanation = calculate_event_quality(los, flight1, flight2)
    print(f"Short track: quality={quality}, explanation={explanation}")
    assert quality == 'low'
    assert 'short track' in explanation.lower() or 'insufficient' in explanation.lower()

    # Test 3: Moderate duration (30–60s) → medium
    los = _make_los(100.0, 150.0)  # 50 seconds

    flight1 = _make_flight('N123AB', 50.0, 300.0)
    flight2 = _make_flight('N456CD', 50.0, 300.0)

    quality, explanation = calculate_event_quality(los, flight1, flight2)
    print(f"Moderate duration: quality={quality}, explanation={explanation}")
    assert quality == 'medium'
    assert 'moderate duration' in explanation.lower()

    # Test 4: Helicopter → low
    los = _make_los(100.0, 130.0)  # 30 seconds

    flight1 = _make_flight('N123AB', 50.0, 300.0, category='A7')
    flight2 = _make_flight('N456CD', 50.0, 300.0)

    quality, explanation = calculate_event_quality(los, flight1, flight2)
    print(f"Helicopter: quality={quality}, explanation={explanation}")
    assert quality == 'low'
    assert 'helicopter' in explanation.lower()

    # Test 5: High quality (≤ 30s, good tracks, no helicopter)
    los = _make_los(100.0, 130.0)  # 30 seconds

    flight1 = _make_flight('N123AB', 50.0, 300.0)
    flight2 = _make_flight('N456CD', 50.0, 300.0)

    quality, explanation = calculate_event_quality(los, flight1, flight2)
    print(f"High quality: quality={quality}, explanation={explanation}")
    assert quality == 'high'
    assert 'brief' in explanation.lower() or 'good' in explanation.lower()
