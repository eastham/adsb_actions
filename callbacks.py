import logging
from flight import Flight
from stats import Stats

class Callbacks:
    @classmethod
    def add_op(cls, flight: Flight):
        logging.debug("In add_op %s", flight.flight_id)

    @classmethod
    def empty_callback(cls, flight: Flight):
        """null callback for testing purposes."""
        pass
