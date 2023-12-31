import logging
import callbacks
import adsbactions

def set_all_loggers(level):
    # turn down loggers systemwide for noise/perf reasons
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    for logger in loggers:
        if logger.level < level: logger.setLevel(level)

def setup_test_callback(aa: adsbactions.AdsbActions):
    aa.register_callback("test_callback", callbacks.Callbacks.test_callback)