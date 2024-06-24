"""Module to call out to external database."""
import datetime
import logging
from adsb_actions.adsb_logger import Logger

logger = logging.getLogger(__name__)
#logger.level = logging.DEBUG
LOGGER = Logger()

USE_APPSHEET = True
TZ_CONVERT = 0  # -7  # UTC conversion for outgoing ops

class Database:
    """Abstraction layer for different database backends."""

    def __init__(self):
        self.lookup_db_call = None
        self.add_op_db_call = None
        self.add_aircraft_db_call = None
        self.add_abe_call = None
        self.update_abe_call = None
        self.enter_fake_mode = None

        if USE_APPSHEET:
            self.appsheet_setup()
        else:
            # add other dbs here
            pass

    def appsheet_setup(self):
        from db import appsheet_api

        APPSHEET = appsheet_api.Appsheet()
        self.lookup_db_call = APPSHEET.aircraft_lookup
        self.add_op_db_call = APPSHEET.add_op
        self.add_aircraft_db_call = APPSHEET.add_aircraft
        self.add_abe_call = APPSHEET.add_cpe
        self.update_abe_call = APPSHEET.update_cpe
        self.enter_fake_mode = APPSHEET.enter_fake_mode

DATABASE = Database()

def add_op(flight, op, flags):
    flight_name = flight.flight_id.strip()
    flighttime = datetime.datetime.fromtimestamp(flight.lastloc.now + 7*60*60)
    logger.debug("add_op %s %s at %s", op, flight_name,
                 flighttime.strftime('%H:%M %d'))

    aircraft_internal_id = lookup_or_create_aircraft(flight)

    DATABASE.add_op_db_call(aircraft_internal_id, flight.lastloc.now + TZ_CONVERT*60*60,
        flags, op, flight_name)

def lookup_or_create_aircraft(flight):
    """
    Return database internal id for flight, checking w/ server if needed,
    creating if needed.
    """

    # check local cache
    if flight.external_id:
        return flight.external_id

    # id not cached locally
    with flight.threadlock:
        if flight.external_id:  # recheck in case we were preempted
            return flight.external_id
        aircraft_external_id = DATABASE.lookup_db_call(flight.flight_id)

        if not aircraft_external_id:
            aircraft_external_id = DATABASE.add_aircraft_db_call(flight.flight_id)
            logger.debug("LOOKUP added aircraft and now has aircraft_external_id %s", 
                         aircraft_external_id)
        else:
            logger.debug("LOOKUP got cached aircraft_external_id %s", aircraft_external_id)

        flight.external_id = aircraft_external_id

    return flight.external_id

def add_abe(flight1, flight2, latdist, altdist):
    flight1_internal_id = lookup_or_create_aircraft(flight1)
    flight2_internal_id = lookup_or_create_aircraft(flight2)

    return DATABASE.add_abe_call(flight1_internal_id, flight2_internal_id,
        latdist, altdist, flight1.lastloc.now, flight1.lastloc.lat, 
        flight1.lastloc.lon)

def update_abe(flight1, flight2, latdist, altdist, create_time, id):
    flight1_internal_id = lookup_or_create_aircraft(flight1)
    flight2_internal_id = lookup_or_create_aircraft(flight2)
    DATABASE.update_abe_call(flight1_internal_id, flight2_internal_id,
                            latdist, altdist, create_time, id)
