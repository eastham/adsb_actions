"""Abstract interface for flight database backends.

This module defines the DatabaseInterface ABC that all database backends
must implement. It also provides NullDatabase, a no-op implementation
for when no database is configured.

To create a custom database backend:
1. Subclass DatabaseInterface
2. Implement all abstract methods
3. Use set_database() to register your implementation

Example:
    from core.database.interface import DatabaseInterface, set_database

    class MyDatabase(DatabaseInterface):
        def aircraft_lookup(self, tail, wholeobj=False):
            # Your implementation here
            return my_db.find(tail)
        # ... implement other methods

    set_database(MyDatabase())
"""

from abc import ABC, abstractmethod
from typing import Optional, Any
import logging

logger = logging.getLogger(__name__)

# Module-level database instance
_database: 'DatabaseInterface' = None


class DatabaseInterface(ABC):
    """Abstract interface for flight database backends.

    Implement this class to connect adsb-actions to your own database.
    See AppsheetDatabase in appsheet_api.py for a reference implementation.
    """

    @abstractmethod
    def aircraft_lookup(self, tail: str, wholeobj: bool = False) -> Optional[Any]:
        """Look up aircraft by tail number.

        Args:
            tail: Aircraft tail number (e.g., "N12345")
            wholeobj: If True, return the full database object.
                     If False, return just the row ID.

        Returns:
            Row ID string, full object dict, or None if not found.
        """
        ...

    @abstractmethod
    def pilot_lookup(self, rowid: str) -> Optional[dict]:
        """Look up pilot by database row ID.

        Args:
            rowid: Database row ID for the pilot

        Returns:
            Pilot dict or None if not found.
        """
        ...

    @abstractmethod
    def add_aircraft(self, regno: str, **kwargs) -> Optional[str]:
        """Add new aircraft to the database.

        Args:
            regno: Aircraft registration number
            **kwargs: Additional aircraft properties

        Returns:
            Row ID of the created record, or None on failure.
        """
        ...

    @abstractmethod
    def add_op(self, aircraft_id: str, time: float, scenic: bool,
               optype: str, flight_name: str) -> bool:
        """Log an operation (arrival/departure).

        Args:
            aircraft_id: Database row ID of the aircraft
            time: Unix timestamp of the operation
            scenic: Whether this is a scenic flight
            optype: Type of operation (e.g., "Arrival", "Departure")
            flight_name: Flight identifier/name

        Returns:
            True on success, False on failure.
        """
        ...

    @abstractmethod
    def add_los(self, flight1_id: str, flight2_id: str, latdist: float,
                altdist: float, time: float, lat: float, lon: float) -> Optional[str]:
        """Log a loss-of-separation event.

        Args:
            flight1_id: Database row ID of first aircraft
            flight2_id: Database row ID of second aircraft
            latdist: Lateral separation in nautical miles
            altdist: Altitude separation in feet
            time: Unix timestamp of the event
            lat: Latitude of the event
            lon: Longitude of the event

        Returns:
            Row ID of the created record, or None on failure.
        """
        ...

    @abstractmethod
    def update_los(self, flight1_id: str, flight2_id: str, latdist: float,
                   altdist: float, time: float, rowid: str) -> Any:
        """Update an existing LOS record with final values.

        Args:
            flight1_id: Database row ID of first aircraft
            flight2_id: Database row ID of second aircraft
            latdist: Final lateral separation in nautical miles
            altdist: Final altitude separation in feet
            time: Unix timestamp
            rowid: Row ID of the LOS record to update

        Returns:
            Updated record or None on failure.
        """
        ...

    def enter_fake_mode(self):
        """Enter fake/test mode where no actual database calls are made.

        Optional - override if your implementation supports this.
        """
        pass


class NullDatabase(DatabaseInterface):
    """No-op database for when no backend is configured.

    All operations succeed silently. Use this as the default when
    no database integration is needed.
    """

    def aircraft_lookup(self, tail: str, wholeobj: bool = False) -> Optional[Any]:
        logger.debug("NullDatabase: aircraft_lookup(%s)", tail)
        return None

    def pilot_lookup(self, rowid: str) -> Optional[dict]:
        logger.debug("NullDatabase: pilot_lookup(%s)", rowid)
        return None

    def add_aircraft(self, regno: str, **kwargs) -> Optional[str]:
        logger.debug("NullDatabase: add_aircraft(%s)", regno)
        return "null_id"

    def add_op(self, aircraft_id: str, time: float, scenic: bool,
               optype: str, flight_name: str) -> bool:
        logger.debug("NullDatabase: add_op(%s, %s)", aircraft_id, optype)
        return True

    def add_los(self, flight1_id: str, flight2_id: str, latdist: float,
                altdist: float, time: float, lat: float, lon: float) -> Optional[str]:
        logger.debug("NullDatabase: add_los(%s, %s)", flight1_id, flight2_id)
        return "null_id"

    def update_los(self, flight1_id: str, flight2_id: str, latdist: float,
                   altdist: float, time: float, rowid: str) -> Any:
        logger.debug("NullDatabase: update_los(%s)", rowid)
        return None


def get_database() -> DatabaseInterface:
    """Get the current database instance.

    Returns:
        The configured DatabaseInterface, or NullDatabase if none set.
    """
    global _database
    if _database is None:
        _database = NullDatabase()
        logger.debug("No database configured, using NullDatabase")
    return _database


def set_database(db: DatabaseInterface) -> None:
    """Set the database backend to use.

    Call this during application initialization to configure your
    database backend.

    Args:
        db: A DatabaseInterface implementation instance.

    Example:
        from core.database.interface import set_database
        from core.database.appsheet import AppsheetDatabase

        set_database(AppsheetDatabase(use_fake_calls=False))
    """
    global _database
    logger.info("Setting database backend: %s", type(db).__name__)
    _database = db
