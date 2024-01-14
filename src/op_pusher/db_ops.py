"""Module to call out to external database for potential on-screen updates."""
import logging
import sys
import datetime
logger = logging.getLogger(__name__)

TZ_CONVERT = 0 # -7  # UTC conversion

USE_APPSHEET = True
if USE_APPSHEET:
    sys.path.insert(0, '../db')
    import appsheet_api
    APPSHEET = appsheet_api.Appsheet()
    LOOKUP_DB_CALL = APPSHEET.aircraft_lookup
    ADD_OP_DB_CALL = APPSHEET.add_op
    ADD_AIRCRAFT_DB_CALL = APPSHEET.add_aircraft
    ADD_ABE_CALL = APPSHEET.add_cpe
    UPDATE_ABE_CALL = APPSHEET.update_cpe
else:
    # add other dbs here
    pass

def add_op(flight, op, flags):
    flight_name = flight.flight_id.strip()
    flighttime = datetime.datetime.fromtimestamp(flight.lastloc.now + 7*60*60)
    logger.debug("add_op %s %s at %s", op, flight_name, 
                 flighttime.strftime('%H:%M %d'))

    aircraft_internal_id = lookup_or_create_aircraft(flight)

    ADD_OP_DB_CALL(aircraft_internal_id, flight.lastloc.now + TZ_CONVERT*60*60,
        flags, op, flight_name)

def lookup_or_create_aircraft(flight):
    """
    Return database internal id for flight, checking w/ server if needed,
    creating if needed.
    """

    flight_id = flight.tail
    if not flight_id:
        flight_id = flight.flight_id

    # local cache
    if flight.external_id:
        return flight.external_id

    # id not cached locally
    with flight.threadlock:
        if flight.external_id:  # recheck in case we were preempted
            return flight.external_id
        aircraft_external_id = LOOKUP_DB_CALL(flight_id)

        if not aircraft_external_id:
            aircraft_external_id = ADD_AIRCRAFT_DB_CALL(flight_id)
            logger.debug("LOOKUP added aircraft and now has aircraft_external_id %s" % 
                         aircraft_external_id)
        else:
            logger.debug("LOOKUP got cached aircraft_external_id %s" % aircraft_external_id)

        flight.external_id = aircraft_external_id

    return flight.external_id

def add_abe(flight1, flight2, latdist, altdist):
    flight1_internal_id = lookup_or_create_aircraft(flight1)
    flight2_internal_id = lookup_or_create_aircraft(flight2)

    return ADD_ABE_CALL(flight1_internal_id, flight2_internal_id,
        latdist, altdist, flight1.lastloc.now, flight1.lastloc.lat, 
        flight1.lastloc.lon)

def update_abe(flight1, flight2, latdist, altdist, create_time, id):
    flight1_internal_id = lookup_or_create_aircraft(flight1)
    flight2_internal_id = lookup_or_create_aircraft(flight2)

    UPDATE_ABE_CALL(flight1_internal_id, flight2_internal_id,
                    latdist, altdist, create_time, id)
