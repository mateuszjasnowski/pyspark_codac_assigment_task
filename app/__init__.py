"""
App initial file
"""
import logging
import os

from logging.handlers import RotatingFileHandler
from pyspark.sql import SparkSession

# logging settings
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)  # TODO change to INFO if DEBUG not needed

_log_formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")
_LOG_PATH = "./logs/codac_assigment_log.log"
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)

log_size_rotating_handler = RotatingFileHandler(
    filename=_LOG_PATH, mode="a", maxBytes=100 * 10, backupCount=5
)
log_size_rotating_handler.setFormatter(_log_formatter)

LOGGER.addHandler(log_size_rotating_handler)

class AppSession():
    """AppSession class"""

    def __init__(self, app_name: str = "CodacApp") -> None:
        """Inizialize app, create session"""

        self.sp_session = SparkSession.builder.appName(app_name).getOrCreate()
        LOGGER.info("Starting app session names %s", app_name)

    def exit(self) -> None:
        """Terminate app's session"""
        LOGGER.info("Stopping app's session")
        self.sp_session.stop()
