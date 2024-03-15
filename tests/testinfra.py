import logging

from adsb_actions.stats import Stats
from adsb_actions.adsbactions import AdsbActions

def set_all_loggers(level):
    # turn down loggers systemwide for noise/perf reasons
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    for logger in loggers:
        if logger.level < level: logger.setLevel(level)

def test_callback(_):
    """null callback for testing purposes."""
    pass

def setup_test_callback(aa: AdsbActions):
    aa.register_callback("test_callback", test_callback)

def load_json(fn):
    with open(fn, 'rt', encoding="utf-8") as myfile:
        return myfile.read()
