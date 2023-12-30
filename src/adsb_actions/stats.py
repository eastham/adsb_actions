from flight import Flight

class Stats:
    json_readlines: int = 0
    condition_match_calls: int = 0
    condition_matches_true: int = 0

    callbacks_fired: int = 0
    callbacks_with_notes: int = 0
    last_callback_flight: Flight = None

    slacks_fired: int = 0
    pages_fired: int = 0
    
    @classmethod
    def reset(cl):
        cl.json_readlines = cl.condition_match_calls = 0
        cl.condition_matches_true = cl.callbacks_fired = 0
        cl.callbacks_with_notes = 0
        cl.last_callback_flight = None
        cl.slacks_fired = cl.pages_fired = 0