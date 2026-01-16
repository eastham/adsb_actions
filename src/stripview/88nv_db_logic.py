"""88NV airport-specific database logic for stripview UI.

This module contains the custom logic for interpreting AppSheet database
results for the 88NV/BRC airport context. 

To use in your own application, create a similar module with your own
interpretation logic and pass it as custom_logic_cb to DbInterface.
"""

import logging

logger = logging.getLogger(__name__)


def test_dict(d, key):
    """Returns True if key is in d and the value is not empty/N."""
    if not d:
        return False
    if key not in d:
        return False
    if d[key] == '' or d[key] == 'N':
        return False
    return True


def brc_db_logic(db_obj, pilot_lookup_fn=None):
    """Interpret AppSheet database results for 88NV airport context.

    Args:
        db_obj: Aircraft database object from aircraft_lookup(wholeobj=True)
        pilot_lookup_fn: Optional function to look up pilot info

    Returns:
        Tuple of (note_string, ui_warning, pilot_label, code_label)
        - note_string: Text to display on the flight strip
        - ui_warning: If True, highlight the strip (e.g., red background)
        - pilot_label: Pilot name to display (max 7 chars)
        - code_label: Pilot code to display
    """
    note_string = ""
    ui_warning = False
    pilot_label = None
    code_label = None

    if not db_obj:
        note_string += "* No Reg "
        ui_warning = True
        return note_string, ui_warning, pilot_label, code_label

    # Show arrival count
    if 'Arrivals' in db_obj:
        note_string += "Arrivals=%s " % db_obj['Arrivals']

    # Check for banned aircraft
    if test_dict(db_obj, 'Ban'):
        note_string += "*BANNED "
        ui_warning = True

    # Check arrival count (only for non-BxA aircraft)
    if not test_dict(db_obj, 'IsBxA'):
        arr = db_obj.get('Arrivals', 0)
        try:
            if int(arr) > 2:
                note_string += "* >2 arrivals "
        except (ValueError, TypeError):
            pass

    # Check registration status (skip for BxA and Medevac)
    if not test_dict(db_obj, 'Registered online'):
        if not test_dict(db_obj, 'IsBxA') and not test_dict(db_obj, 'Medevac'):
            note_string += "* No Reg "
            ui_warning = True

    # Check for related notes
    if test_dict(db_obj, 'Related Notes'):
        note_string += "*Notes "

    # Show BxA indicator
    if test_dict(db_obj, 'IsBxA'):
        note_string += "BxA"

    # Look up pilot info if available
    if pilot_lookup_fn and test_dict(db_obj, 'lead pilot'):
        pilot_id = db_obj['lead pilot']
        try:
            pilot_obj = pilot_lookup_fn(pilot_id)
            if pilot_obj:
                if test_dict(pilot_obj, 'Playa name'):
                    pilot_label = pilot_obj.get('Playa name')
                else:
                    pilot_label = pilot_obj.get('Name')
                if pilot_label:
                    pilot_label = pilot_label[0:7]
                code_label = pilot_obj.get('Pilot code')
        except Exception as e:
            logger.debug("Pilot lookup failed: %s", e)

    return note_string, ui_warning, pilot_label, code_label
