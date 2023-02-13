"""
App initial file
"""
import logging
import os
import json

from logging.handlers import RotatingFileHandler

# logging settings
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)  # TODO change to INFO if DEBUG not needed

_log_formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")
_LOG_PATH = "./logs/codac_assigment_log.log"
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)

log_size_rotating_handler = RotatingFileHandler(
    filename=_LOG_PATH, mode="a", maxBytes=50000, backupCount=5
)
log_size_rotating_handler.setFormatter(_log_formatter)

LOGGER.addHandler(log_size_rotating_handler)

# Reading app_config file
with open(
    os.path.dirname(__file__) + "/../app_config.json", "r", encoding="utf-8"
) as config_file:
    APP_CONFIG = json.load(config_file)
