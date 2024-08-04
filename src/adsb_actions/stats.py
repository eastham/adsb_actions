"""Systemwide statistics tracking, mostly for test and debug purposes."""

from prometheus_client import Gauge

class Stats:
    json_readlines: int = 0
    condition_match_calls: int = 0

    callbacks_fired: int = 0
    last_callback_flight = None

    webhooks_fired: int = 0

    # Stats below here come from the op_pusher module, perhaps should be
    # separated out someday.
    takeoffs: int = 0
    popup_takeoffs: int = 0
    landings: int = 0
    local_landings: int = 0

    abe_add: int = 0
    abe_update: int = 0
    abe_finalize: int = 0

    @classmethod
    def reset(cl):
        cl.json_readlines = cl.condition_match_calls = 0
        cl.condition_matches_true = cl.callbacks_fired = 0
        cl.last_callback_flight = None
        cl.takeoffs = cl.popup_takeoffs = 0
        cl.landings = cl.local_landings = 0
        cl.abe_add = cl.abe_update = cl.abe_finalize = 0
        cl.webhooks_fired = 0

    @classmethod
    def register_prom_callbacks(cl):
        """Register a gauge callback for every int member of this class."""

        def make_callback(attr_name):
            """Closure to capture the current attribute name in the for loop."""
            return lambda: getattr(Stats, attr_name)

        for name in dir(cl):
            if name.startswith('_') or not isinstance(getattr(cl, name), int):
                continue
            d = Gauge('adsb_actions_stat_' + name, name)
            d.set_function(make_callback(name))
