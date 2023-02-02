"""AppSession module"""
from pyspark.sql.session import SparkSession
from . import LOGGER

class AppSession:
    """AppSession class"""

    def __init__(self, app_name: str = "CodacApp") -> None:
        """Inizialize app, create session"""
        self.app_name = app_name
        self.sp_session = None

    def __enter__(self) -> SparkSession:
        """Enter to AppSession context manager"""
        self.sp_session = SparkSession.builder.appName(self.app_name).getOrCreate()
        LOGGER.info("Starting app session names %s", self.app_name)

        return self.sp_session

    def __exit__(self, exit_type, value, traceback) -> None:
        """Terminate app's session"""
        LOGGER.debug("Type: %s, Val: %s, Traceb: %s", exit_type, value, traceback)

        LOGGER.info("Stopping app's session")
        self.sp_session.stop()
