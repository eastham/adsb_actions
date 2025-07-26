"""Module to call out to external database for potential on-screen updates."""

import sys

import adsb_logger
from adsb_logger import Logger

logger = adsb_logger.logging.getLogger(__name__)
#logger.level = adsb_logger.logging.DEBUG
LOGGER = Logger()

USE_APPSHEET = True
if USE_APPSHEET:
    sys.path.insert(0, '../db')
    import appsheet_api
    APPSHEET = appsheet_api.Appsheet()
    AIRCRAFT_LOOKUP_DB_CALL = APPSHEET.aircraft_lookup
    PILOT_LOOKUP_DB_CALL = APPSHEET.pilot_lookup

else:
    # add other dbs here
    pass

class DbInterface:
    def __init__(self, flight, ui_update_cb):
        self.flight = flight
        self.ui_update_cb = ui_update_cb

    def call_database(self):
        """Call the remote database to see if we should update the
        on-screen information for the given flight.  Returned information
        is then passed to the UI via callback.
        May block, should be run in own thread. """

        logger.debug("call_database: %s", self.flight.flight_id)

        note_string = ""
        ui_warning = False

        try:
            # TODO could optimize: only if unregistered?
            db_obj = AIRCRAFT_LOOKUP_DB_CALL(self.flight.flight_id, wholeobj=True)
            pilot_label = None
            code_label = None
            ui_warning = False      # turns strip red if True

            if not db_obj:
                note_string += "* No Reg "
                ui_warning = True
            else:
                # take a note of the db's identifier for later UI calls
                self.flight.flags['Row ID'] = db_obj['Row ID']

                note_string += "Arrivals=%s " % db_obj['Arrivals']

                if test_dict(db_obj, 'Ban'):
                    note_string += "*BANNED "
                    ui_warning = True

                if not test_dict(db_obj, 'IsBxA'):
                    arr = db_obj['Arrivals']
                    try:
                        if int(arr) > 2:
                            note_string += "* >2 arrivals "
                    except Exception:
                        pass

                if not test_dict(db_obj, 'Registered online'):
                    if not test_dict(db_obj, 'IsBxA') and not test_dict(db_obj, 'Medevac'):
                        note_string += "* No Reg "
                        ui_warning = True

                if test_dict(db_obj, 'Related Notes'):
                    note_string += "*Notes "

                if test_dict(db_obj, 'IsBxA'):
                    note_string += "BxA"

                if test_dict(db_obj, 'lead pilot'):
                    pilot_id = db_obj['lead pilot']
                    pilot_obj = PILOT_LOOKUP_DB_CALL(pilot_id)
                    if pilot_obj:
                        if test_dict(pilot_obj, 'Playa name'):
                            pilot_label = pilot_obj.get('Playa name')
                        else:
                            pilot_label = pilot_obj.get('Name')
                        if pilot_label:
                            pilot_label = pilot_label[0:7]
                        else:
                            pilot_label = None
                        code_label = pilot_obj.get('Pilot code')

        except Exception as e:
            logger.debug("do_server_update parse failed: " + str(e))
            pass

        logger.debug("call_database complete for %s: note %s warn %d", 
                      self.flight.flight_id, note_string, ui_warning)
        self.ui_update_cb(note_string, ui_warning, pilot_label, code_label,
                          None)

def test_dict(d, key):
    """Returns true if key is in d and that they key's entry is not empty/N"""
    if not d: return False
    if not key in d: return False
    if d[key] == '' or d[key] == 'N': return False
    return True
