"""Module to call out to external database for potential on-screen updates.

This module provides DbInterface, which queries the configured database
for aircraft information and calls a UI update callback with the results.

Custom interpretation logic can be provided via the custom_logic_cb parameter.
See brc_db_logic.py for an example of custom logic (Burning Man specific).
"""

import logging

logger = logging.getLogger(__name__)

# Import database interface - uses NullDatabase by default if not configured
from core.database.interface import get_database


class DbInterface:
    """Interface for querying database and updating UI with results.

    Args:
        flight: The Flight object to look up
        ui_update_cb: Callback function(note, warning, pilot, code, extra)
        custom_logic_cb: Optional callback to interpret database results.
                        If None, uses default logic (just shows if found).
                        Signature: fn(db_obj, pilot_lookup_fn) ->
                                   (note_str, warning, pilot_label, code_label)
    """

    def __init__(self, flight, ui_update_cb, custom_logic_cb=None):
        self.flight = flight
        self.ui_update_cb = ui_update_cb
        self.custom_logic_cb = custom_logic_cb

    def call_database(self):
        """Call the remote database to see if we should update the
        on-screen information for the given flight.  Returned information
        is then passed to the UI via callback.

        May block, should be run in own thread.
        """
        logger.debug("call_database: %s", self.flight.flight_id)

        note_string = ""
        ui_warning = False
        pilot_label = None
        code_label = None

        try:
            db = get_database()
            db_obj = db.aircraft_lookup(self.flight.flight_id, wholeobj=True)

            if db_obj:
                # Store the database row ID for later use
                if 'Row ID' in db_obj:
                    self.flight.flags['Row ID'] = db_obj['Row ID']

            if self.custom_logic_cb:
                # Use custom logic to interpret the database result
                note_string, ui_warning, pilot_label, code_label = \
                    self.custom_logic_cb(db_obj, db.pilot_lookup)
            else:
                # Default logic: just show if aircraft was found
                if db_obj:
                    note_string = "Found in DB"
                    ui_warning = False
                else:
                    note_string = "Not in DB"
                    ui_warning = True

        except Exception as e:  # pylint: disable=broad-except
            logger.debug("call_database failed: %s", str(e))

        logger.debug("call_database complete for %s: note %s warn %s",
                     self.flight.flight_id, note_string, ui_warning)
        self.ui_update_cb(note_string, ui_warning, pilot_label, code_label, None)


def test_dict(d, key):
    """Returns True if key is in d and the value is not empty/N.

    Utility function for checking database result fields.
    """
    if not d:
        return False
    if key not in d:
        return False
    if d[key] == '' or d[key] == 'N':
        return False
    return True
