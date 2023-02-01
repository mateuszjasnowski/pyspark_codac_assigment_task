"""Data module"""
import os

from pyspark.sql import SparkSession

from app import LOGGER

class Data():
    """Data class"""

    def __init__(
        self,
        session: SparkSession,
        file_path: str,
        header: bool = True
        ) -> None:
        """
        Create DataFrame object
        creates data parameters containing spark's DataFrame
        """

        self.data = session.read.option(
            'header',
            str(header).lower()
            ).csv(file_path)
        self.data_frame_name = os.path.basename(file_path)

        LOGGER.info(
            "Created DataFrame (%s) object with %d columns",
            self.data_frame_name,
            len(self.data.columns)
        )

    #def country_filter(self, counties: list) -> None:
        """Filter DataFrame by country"""
    #    for country in counties:
    #        self.data = self.data.filter(
    #            self.data.country == country
    #            )
    #    LOGGER.info("Filterring DataFrame %s by country - ")



#self.data = self.data.filter(self.data.country == "#filter")

#.drop("first_name", "last_name")
