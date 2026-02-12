"""Test quality explanation generation."""

from unittest.mock import Mock
from src.applications.airport_monitor.los import calculate_event_quality


def test_quality_explanations():
    """Demonstrate the various quality explanations."""

    # Test 1: Long event (formation flight)
    los = Mock()
    los.create_time = 100.0
    los.last_time = 250.0  # 150 seconds = 2.5 minutes
    los.min_latdist = 0.5
    los.min_altdist = 500

    flight1 = Mock()
    flight1.firstloc = Mock(now=50.0)
    flight1.lastloc = Mock(now=300.0, flightdict={'category': 'A1'})

    flight2 = Mock()
    flight2.firstloc = Mock(now=50.0)
    flight2.lastloc = Mock(now=300.0, flightdict={'category': 'A1'})

    quality, explanation = calculate_event_quality(los, flight1, flight2)
    print(f"\nLong event: quality={quality}, explanation={explanation}")
    assert quality == 'low'
    assert 'formation' in explanation.lower()

    # Test 2: Short track (insufficient data)
    los = Mock()
    los.create_time = 100.0
    los.last_time = 145.0  # 45 seconds
    los.min_latdist = 0.5
    los.min_altdist = 500

    flight1 = Mock()
    flight1.firstloc = Mock(now=90.0)
    flight1.lastloc = Mock(now=120.0, flightdict={'category': 'A1'})  # 30 second track

    flight2 = Mock()
    flight2.firstloc = Mock(now=50.0)
    flight2.lastloc = Mock(now=300.0, flightdict={'category': 'A1'})

    quality, explanation = calculate_event_quality(los, flight1, flight2)
    print(f"Short track: quality={quality}, explanation={explanation}")
    assert quality == 'low'
    assert 'short track' in explanation.lower() or 'insufficient' in explanation.lower()

    # Test 3: Moderate duration
    los = Mock()
    los.create_time = 100.0
    los.last_time = 190.0  # 90 seconds
    los.min_latdist = 0.5
    los.min_altdist = 500

    flight1 = Mock()
    flight1.firstloc = Mock(now=50.0)
    flight1.lastloc = Mock(now=300.0, flightdict={'category': 'A1'})

    flight2 = Mock()
    flight2.firstloc = Mock(now=50.0)
    flight2.lastloc = Mock(now=300.0, flightdict={'category': 'A1'})

    quality, explanation = calculate_event_quality(los, flight1, flight2)
    print(f"Moderate duration: quality={quality}, explanation={explanation}")
    assert quality == 'medium'
    assert 'moderate duration' in explanation.lower()

    # Test 4: Helicopter
    los = Mock()
    los.create_time = 100.0
    los.last_time = 130.0  # 30 seconds
    los.min_latdist = 0.5
    los.min_altdist = 500

    flight1 = Mock()
    flight1.firstloc = Mock(now=50.0)
    flight1.lastloc = Mock(now=300.0, flightdict={'category': 'A7'})  # Helicopter

    flight2 = Mock()
    flight2.firstloc = Mock(now=50.0)
    flight2.lastloc = Mock(now=300.0, flightdict={'category': 'A1'})

    quality, explanation = calculate_event_quality(los, flight1, flight2)
    print(f"Helicopter: quality={quality}, explanation={explanation}")
    assert quality == 'medium'
    assert 'helicopter' in explanation.lower()

    # Test 5: High quality
    los = Mock()
    los.create_time = 100.0
    los.last_time = 145.0  # 45 seconds
    los.min_latdist = 0.5
    los.min_altdist = 500

    flight1 = Mock()
    flight1.firstloc = Mock(now=50.0)
    flight1.lastloc = Mock(now=300.0, flightdict={'category': 'A1'})

    flight2 = Mock()
    flight2.firstloc = Mock(now=50.0)
    flight2.lastloc = Mock(now=300.0, flightdict={'category': 'A1'})

    quality, explanation = calculate_event_quality(los, flight1, flight2)
    print(f"High quality: quality={quality}, explanation={explanation}")
    assert quality == 'high'
    assert 'brief' in explanation.lower() or 'good' in explanation.lower()
