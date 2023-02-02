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

    def filter(self, column: str, counties: list) -> None:
        """Filter DataFrame by country"""

        self.data = self.data.filter(
            self.data[column].isin(*counties)
        )
        LOGGER.info(
            "Filterring DataFrame %s by column %s == %s",
            self.data_frame_name,
            column,
            str(counties)
            )

    def drop_column(self, columns: list) -> None:
        """Dropping selected columns from DataFrame"""

        self.data = self.data.drop(*columns)
        LOGGER.info(
            "Dropping columns %s from DataFrame %s",
            str(columns),
            self.data_frame_name
        )
