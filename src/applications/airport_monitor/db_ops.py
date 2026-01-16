"""Module to call out to external database.

This module provides helper functions for database operations, using
the configured DatabaseInterface backend.

To configure a database backend:
    from core.database.interface import set_database
    from core.database.appsheet import AppsheetDatabase

    set_database(AppsheetDatabase(use_fake_calls=False))

If no database is configured, NullDatabase is used (all ops succeed silently).
"""

import datetime
import logging
from adsb_actions.adsb_logger import Logger
from core.database.interface import get_database

logger = logging.getLogger(__name__)
#logger.level = logging.DEBUG
LOGGER = Logger()

TZ_CONVERT = 0  # UTC conversion for outgoing ops (e.g., -7 for PDT)


def add_op(flight, op, flags):
    """Add an operation (arrival/departure) to the database."""
    flight_name = flight.flight_id.strip()
    flighttime = datetime.datetime.fromtimestamp(flight.lastloc.now + 7*60*60)
    logger.debug("add_op %s %s at %s", op, flight_name,
                 flighttime.strftime('%H:%M %d'))

    aircraft_internal_id = lookup_or_create_aircraft(flight)

    get_database().add_op(aircraft_internal_id,
                          flight.lastloc.now + TZ_CONVERT*60*60,
                          flags, op, flight_name)


def lookup_or_create_aircraft(flight):
    """Return database internal id for flight, creating if needed.

    Uses local caching on the flight object to avoid repeated lookups.
    """
    # check local cache
    if flight.external_id:
        return flight.external_id

    # id not cached locally
    with flight.threadlock:
        if flight.external_id:  # recheck in case we were preempted
            return flight.external_id

        db = get_database()
        aircraft_external_id = db.aircraft_lookup(flight.flight_id)

        if not aircraft_external_id:
            aircraft_external_id = db.add_aircraft(flight.flight_id)
            logger.debug("LOOKUP added aircraft and now has aircraft_external_id %s",
                         aircraft_external_id)
        else:
            logger.debug("LOOKUP got cached aircraft_external_id %s", aircraft_external_id)

        flight.external_id = aircraft_external_id

    return flight.external_id


def add_los(flight1, flight2, latdist, altdist):
    """Add a loss-of-separation event to the database."""
    flight1_internal_id = lookup_or_create_aircraft(flight1)
    flight2_internal_id = lookup_or_create_aircraft(flight2)

    return get_database().add_los(flight1_internal_id, flight2_internal_id,
                                  latdist, altdist, flight1.lastloc.now,
                                  flight1.lastloc.lat, flight1.lastloc.lon)


def update_los(flight1, flight2, latdist, altdist, create_time, rowid):
    """Update an existing LOS record with final values."""
    flight1_internal_id = lookup_or_create_aircraft(flight1)
    flight2_internal_id = lookup_or_create_aircraft(flight2)

    return get_database().update_los(flight1_internal_id, flight2_internal_id,
                                     latdist, altdist, create_time, rowid)
