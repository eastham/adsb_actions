"""Module to call out to external database for potential on-screen updates."""
import logging
import sys
logger = logging.getLogger(__name__)
logger.level = logging.DEBUG

USE_APPSHEET = True
if USE_APPSHEET:
    sys.path.insert(0, '../db')
    import appsheet_api
    APPSHEET = appsheet_api.Appsheet()
    LOOKUP_DB_CALL = APPSHEET.aircraft_lookup
else:
    # add other dbs here
    pass

class DbInterface:
    def __init__(self, flight, ui_update_cb):
        self.flight = flight
        self.ui_update_cb = ui_update_cb

    def call_database(self):
        """Call the remote database to see if we should update the
        on-screen information for the given flight.
        May block, should be run in own thread. """

        logging.debug("call_database: %s", self.flight.tail)

        note_string = ""
        ui_warning = False

        try:
            # TODO could optimize: only if unregistered?
            # TODO move appsheet code to another module for cleanliness
            db_obj = LOOKUP_DB_CALL(self.flight.tail, wholeobj=True)

            ui_warning = False

            if not db_obj:
                note_string += "*Unreg "
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
                    if not test_dict(db_obj, 'IsBxA'):
                        note_string += "*Manual reg "
                        ui_warning = True

                if test_dict(db_obj, 'Related Notes'):
                    note_string += "*Notes "

            if test_dict(db_obj, 'IsBxA'):
                note_string += "BxA"

        except Exception as e:
            logging.debug("do_server_update parse failed: " + str(e))
            pass

        logging.debug("call_database complete for %s: note %s warn %d", 
                      self.flight.tail, note_string, ui_warning)
        self.ui_update_cb(note_string, ui_warning)

def test_dict(d, key):
    """Returns true if key is in d and that they key's entry is not empty/N"""
    if not d: return False
    if not key in d: return False
    if d[key] == '' or d[key] == 'N': return False
    return True
