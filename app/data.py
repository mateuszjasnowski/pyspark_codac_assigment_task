"""Data module"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.utils import IllegalArgumentException

from app import LOGGER


class Data:
    """Data class"""

    def __init__(
        self, session: SparkSession, file_path: str, header: bool = True
    ) -> None:
        """
        Create DataFrame object
        creates data parameters containing spark's DataFrame
        """
        self.data = session.read.option("header", str(header).lower()).csv(file_path)
        self.data_frame_name = os.path.basename(file_path)
        self.session = session

        LOGGER.info(
            "Created DataFrame (%s) object with %d columns",
            self.data_frame_name,
            len(self.data.columns),
        )

    def filter(self, column: str, match: list) -> None:
        """Filter DataFrame by column match to given value"""

        self.data = self.data.filter(self.data[column].isin(*match))
        LOGGER.info(
            "Filterring DataFrame %s by column %s == %s",
            self.data_frame_name,
            column,
            str(match),
        )

    def drop_column(self, columns: list) -> None:
        """Dropping selected columns from DataFrame"""

        self.data = self.data.drop(*columns)
        LOGGER.info(
            "Dropping columns %s from DataFrame %s", str(columns), self.data_frame_name
        )

    def join_data(
        self, join_dataset_path: str, first_pk: str = "id", second_pk: str = "id"
    ) -> None:
        """Join second DataFrame, method returns new Data object"""

        dataset_to_join = Data(self.session, join_dataset_path)

        self.data = self.data.join(
            dataset_to_join.data, self.data[first_pk] == dataset_to_join.data[second_pk]
        ).drop(dataset_to_join.data[second_pk])
        LOGGER.info(
            "Joining DataFrame %s to DataFrame %s on %s == %s",
            dataset_to_join.data_frame_name,
            self.data_frame_name,
            first_pk,
            second_pk,
        )

    def rename_columns(self, new_columns: list) -> None:
        """Change name of columns on DataFrame"""

        current_columns = self.data.columns

        try:
            self.data = self.data.toDF(*new_columns)
            LOGGER.info(
                "Renaming %d columns (%s) to (%s)",
                len(new_columns),
                str(current_columns),
                new_columns,
            )
        except IllegalArgumentException as error:
            LOGGER.warning("%s", error)

    def save(self) -> None:
        """Save DataFrame into new file"""
        master_path = os.path.abspath(os.getcwd())

        self.data.write.csv(f"file://{master_path}/client_data/", mode="overwrite", header=True)
        LOGGER.info("Writing DataFrame to file at ./client_data/")
