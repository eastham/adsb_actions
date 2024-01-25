"""Systemwide statistics tracking, mostly for test and debug purposes."""

class Stats:
    json_readlines: int = 0
    condition_match_calls: int = 0

    callbacks_fired: int = 0
    last_callback_flight = None

    webhooks_fired: int = 0

    flight_annotates: int = 0
    takeoffs: int = 0
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
        cl.webhooks_fired = 0