import logging

from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

#logging settings
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG) #TODO change to INFO if DEBUG not needed

_log_formatter = logging.Formatter('%(asctime)s;%(levelname)s;%(message)s')
_log_path = './logs/codac_assigment_log.log'
#log_filehandler = logging.FileHandler('./logs/codac_assigment_log.log')

'''log_day_rotating_handler = TimedRotatingFileHandler(
    filename = _log_path,
    when='D',
    interval=5,
    backupCount=5,
)
log_day_rotating_handler.setFormatter(_log_formatter)'''

log_size_rotating_handler = RotatingFileHandler(
    filename=_log_path,
    mode='a',
    maxBytes=100*10,
    backupCount=5
)
log_size_rotating_handler.setFormatter(_log_formatter)

LOGGER.addHandler(log_size_rotating_handler)

