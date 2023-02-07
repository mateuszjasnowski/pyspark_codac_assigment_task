"""AppSession module"""
import os
from pyspark.sql.session import SparkSession
from . import LOGGER

class AppSession:
    """AppSession class"""

    def __init__(self, app_name: str = "CodacApp", env_var: bool = True, master: str = None) -> None:
        """Inizialize app, create session"""
        self.app_name = app_name
        self.sp_session = None

        if not env_var and not master:
            raise ValueError("Missing Spark master address argument when env_var = False")
        if env_var:
            self.master = os.environ.get('SPARK_MASTER')
        else:
            self.master = master

    def __enter__(self) -> SparkSession:
        """Enter to AppSession context manager"""
        self.sp_session = SparkSession.builder.master(
            self.master
            ).appName(self.app_name).getOrCreate()
        LOGGER.info("Starting app session names %s", self.app_name)

        return self.sp_session

    def __exit__(self, exit_type, value, traceback) -> None:
        """Terminate app's session"""
        LOGGER.debug("Type: %s, Val: %s, Traceb: %s", exit_type, value, traceback)

        LOGGER.info("Stopping app's session")
        self.sp_session.stop()
        LOGGER.info("====================EOL====================")
