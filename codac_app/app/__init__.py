"""
App initial file
"""
import logging
import os

from logging.handlers import RotatingFileHandler

# logging settings
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

_log_formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")
_LOG_PATH = "./codac_app/logs/codac_assigment_log.log"
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)

log_size_rotating_handler = RotatingFileHandler(
    filename=_LOG_PATH, mode="a", maxBytes=50000, backupCount=5
)
log_size_rotating_handler.setFormatter(_log_formatter)

LOGGER.addHandler(log_size_rotating_handler)
