"""Data module"""
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import IllegalArgumentException

from . import LOGGER


class Data:
    """Data class"""

    def __init__(
        self, session: SparkSession, file_path: str, header: bool = True
    ) -> None:
        """
        Create DataFrame object
        creates data parameters containing spark's DataFrame
        """
        LOGGER.debug("Data.__init__")
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

        LOGGER.debug("Data.filter")
        if column in self.data.columns:
            self.data = self.data.filter(self.data[column].isin(*match))
            LOGGER.info(
                "Filterring DataFrame %s by column %s == %s",
                self.data_frame_name,
                column,
                str(match),
            )

    def drop_column(self, columns: list) -> None:
        """Dropping selected columns from DataFrame"""

        LOGGER.debug("Data.drop_column")
        self.data = self.data.drop(*columns)
        LOGGER.info(
            "Dropping columns %s from DataFrame %s", str(columns), self.data_frame_name
        )

    def join_data(
        self,
        join_dataset: DataFrame,
        join_dataset_name: str,
        first_pk: str,
        second_pk: str,
    ) -> None:
        """Join second DataFrame, method returns new Data object"""

        LOGGER.debug("Data.join_data")
        self.data = self.data.join(
            join_dataset, self.data[first_pk] == join_dataset[second_pk]
        ).drop(join_dataset[second_pk])
        LOGGER.info(
            "Joining DataFrame %s to DataFrame %s on %s == %s",
            join_dataset_name,
            self.data_frame_name,
            first_pk,
            second_pk,
        )

    def rename(self, old_col: str, new_col: str) -> None:
        """Rename column if exists in DataFrame"""

        LOGGER.debug("Data.rename")
        if old_col in self.data.columns:
            columns = self.data.columns
            columns[columns.index(old_col)] = new_col

            self.data = self.data.toDF(*columns)
            LOGGER.info(
                "Renaming column %s to %s in %s", old_col, new_col, self.data_frame_name
            )

    def rename_columns(self, new_columns: list) -> None:
        """Change name of all columns on DataFrame"""

        LOGGER.debug("Data.rename_columns")
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

    def save(
        self,
        file_format: str,
        subfolder_path: str = "./client_data/",
        header: str = "true",
        save_mode: str = "overwrite",
    ) -> None:
        """
        Save DataFrame into new file
        """

        LOGGER.debug("Data.save")
        save_path = f"file://{os.path.abspath(os.getcwd())}/{subfolder_path}"

        LOGGER.info("Saving %s file at location: %s", file_format, subfolder_path)

        self.data.write.option("header", header).mode(save_mode).save(
            save_path, format=file_format
        )

        LOGGER.info("File saved succesfully!")
