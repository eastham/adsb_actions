# This module configures logging for adsb_actions modules

# To use it, set up the following in the top of your module
# note you can control logger.level at module level
# if logger.level is not set in the module, the level is set here
#
# import adsb_logger
# from adsb_logger import Logger
# logger = adsb_logger.logging.getLogger(__name__)
# #logger.level = adsb_logger.logging.DEBUG
# LOGGER = Logger()
#
# then, in code, use 
# logger.INFO(message)
#

import logging
import logging.handlers

logger = logging.getLogger(__name__)

log_level = logging.INFO

class Logger:
  def __init__(self):
    logging.basicConfig (
        level=log_level,
        format='%(asctime)s %(levelname)s adsb_actions %(module)s:%(lineno)d: %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.handlers.SysLogHandler()
#            logging.handlers.SysLogHandler(address='/dev/log')
    #        logging.FileHandler("log/op_pusher.log"),
        ]
    )

