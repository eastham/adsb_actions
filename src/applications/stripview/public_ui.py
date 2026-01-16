"""A variant of the stripview UI for public consumption.
Hides traffic alerts, adds aliases for aircraft."""

from adsb_actions.flight import Flight
import controller

ALIASES = {
    "N78888": "Dusty"
}

def public_update_cb(f: Flight):
    if f.flight_id in ALIASES:
        f.other_id = ALIASES[f.flight_id]

if __name__ == '__main__':
    adsb_actions, app = controller.setup(None, None)
    adsb_actions.register_callback("public_update_cb", public_update_cb)
    app.run()
