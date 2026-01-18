"""Example callbacks for low_altitude_alert.yaml.

Usage: python3 src/analyzers/callback_runner.py --data tests/sample_readsb_data \
    --callback_definitions examples/low_altitude_callbacks.py examples/low_altitude_alert.yaml
"""

import logging
import datetime

logger = logging.getLogger(__name__)
# logger.level = logging.DEBUG

def low_altitude_alert_cb(flight):
    """Called when an aircraft matches the low altitude rule."""
    utcstring = datetime.datetime.fromtimestamp(flight.lastloc.now,
                                                datetime.UTC)

    logger.info(f"Low-altitude callback: {flight.flight_id} at {utcstring} alt "
                f"{flight.lastloc.alt_baro} ft")
