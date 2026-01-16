import logging
import datetime

logger = logging.getLogger(__name__)
# logger.level = logging.DEBUG

def altitude_alert_cb(flight1):
    """Called by function name when the rule in example_rules.yaml matches"""
    utcstring = datetime.datetime.fromtimestamp(flight1.lastloc.now,
                                                datetime.UTC)

    logger.info(f"Low-altitude callback: {flight1.flight_id} at {utcstring} alt "
                f"{flight1.lastloc.alt_baro} ft")
