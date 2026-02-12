"""Test flightdict persistence across location updates."""

from unittest.mock import Mock
from src.adsb_actions.flight import Flight
from src.adsb_actions.location import Location


class TestFlightdictPersistence:
    """Test that flightdict is preserved across location updates."""

    def test_flightdict_preserved_when_missing(self):
        """When a new location lacks flightdict, it should be preserved from previous location."""
        # Create a flight with initial location containing flightdict
        initial_loc = Location(
            lat=37.5,
            lon=-122.0,
            alt_baro=1000,
            now=100.0,
            track=90,
            flightdict={'category': 'A7', 'squawk': '1200', 'emergency': 'none'}
        )

        flight = Flight(
            flight_id='N123AB',
            other_id='N123AB',
            firstloc=initial_loc,
            lastloc=initial_loc,
            all_bboxes_list=[]
        )

        # Verify initial flightdict is present
        assert flight.lastloc.flightdict is not None
        assert flight.lastloc.flightdict['category'] == 'A7'
        assert flight.lastloc.flightdict['squawk'] == '1200'

        # Update with a new location that lacks flightdict
        new_loc = Location(
            lat=37.6,
            lon=-122.1,
            alt_baro=1100,
            now=110.0,
            track=95,
            flightdict=None
        )

        flight.update_loc(new_loc)

        # Verify flightdict was preserved
        assert flight.lastloc.flightdict is not None
        assert flight.lastloc.flightdict['category'] == 'A7'
        assert flight.lastloc.flightdict['squawk'] == '1200'
        assert flight.lastloc.flightdict['emergency'] == 'none'

    def test_flightdict_updated_when_present(self):
        """When a new location has flightdict, it should replace the old one."""
        # Create a flight with initial location containing flightdict
        initial_loc = Location(
            lat=37.5,
            lon=-122.0,
            alt_baro=1000,
            now=100.0,
            track=90,
            flightdict={'category': 'A1', 'squawk': '1200'}
        )

        flight = Flight(
            flight_id='N123AB',
            other_id='N123AB',
            firstloc=initial_loc,
            lastloc=initial_loc,
            all_bboxes_list=[]
        )

        # Update with a new location that has different flightdict
        new_loc = Location(
            lat=37.6,
            lon=-122.1,
            alt_baro=1100,
            now=110.0,
            track=95,
            flightdict={'category': 'A7', 'squawk': '7700', 'emergency': 'general'}
        )

        flight.update_loc(new_loc)

        # Verify flightdict was updated to new values
        assert flight.lastloc.flightdict is not None
        assert flight.lastloc.flightdict['category'] == 'A7'
        assert flight.lastloc.flightdict['squawk'] == '7700'
        assert flight.lastloc.flightdict['emergency'] == 'general'

    def test_initial_location_without_flightdict(self):
        """When initial location has no flightdict, it should remain None."""
        # Create a flight with initial location lacking flightdict
        initial_loc = Location(
            lat=37.5,
            lon=-122.0,
            alt_baro=1000,
            now=100.0,
            track=90,
            flightdict=None
        )

        flight = Flight(
            flight_id='N123AB',
            other_id='N123AB',
            firstloc=initial_loc,
            lastloc=initial_loc,
            all_bboxes_list=[]
        )

        # Update with another location without flightdict
        new_loc = Location(
            lat=37.6,
            lon=-122.1,
            alt_baro=1100,
            now=110.0,
            track=95,
            flightdict=None
        )

        flight.update_loc(new_loc)

        # Verify flightdict remains None
        assert flight.lastloc.flightdict is None

    def test_flightdict_appears_later(self):
        """When flightdict first appears in a later update, it should be stored."""
        # Create a flight with initial location lacking flightdict
        initial_loc = Location(
            lat=37.5,
            lon=-122.0,
            alt_baro=1000,
            now=100.0,
            track=90,
            flightdict=None
        )

        flight = Flight(
            flight_id='N123AB',
            other_id='N123AB',
            firstloc=initial_loc,
            lastloc=initial_loc,
            all_bboxes_list=[]
        )

        # First update without flightdict
        update1 = Location(
            lat=37.6,
            lon=-122.1,
            alt_baro=1100,
            now=110.0,
            track=95,
            flightdict=None
        )
        flight.update_loc(update1)
        assert flight.lastloc.flightdict is None

        # Second update WITH flightdict (first time we see it)
        update2 = Location(
            lat=37.7,
            lon=-122.2,
            alt_baro=1200,
            now=120.0,
            track=100,
            flightdict={'category': 'A1', 'squawk': '1200'}
        )
        flight.update_loc(update2)
        assert flight.lastloc.flightdict is not None
        assert flight.lastloc.flightdict['category'] == 'A1'

        # Third update without flightdict - should preserve from update2
        update3 = Location(
            lat=37.8,
            lon=-122.3,
            alt_baro=1300,
            now=130.0,
            track=105,
            flightdict=None
        )
        flight.update_loc(update3)
        assert flight.lastloc.flightdict is not None
        assert flight.lastloc.flightdict['category'] == 'A1'
        assert flight.lastloc.flightdict['squawk'] == '1200'
