""" Test module of app.data.Data.rename_columns() method"""
import chispa
import pytest
from pyspark.sql import SparkSession

from codac_app.app.data import Data


class TestDataRenameColumns:
    """Testing class to test Data.rename_columns() method"""

    @pytest.fixture()
    def spark(self) -> SparkSession:
        """Declaring SparkSession for Chispa testing"""
        return SparkSession.builder.master("local").appName("ChispaTest").getOrCreate()

    @pytest.fixture()
    def dataset_object(self, spark: SparkSession) -> Data:
        """Setting up Data object for test"""
        return Data(spark, "./tests/test_set/dataset_one.csv", header=True)

    def test_data_rename_correct_scenario(
        self, dataset_object: Data, spark: SparkSession
    ) -> None:
        """Testing Data.rename_columns() successful renaming with given all needed column names"""

        dataset_object.rename_columns(["col1", "col2", "col3", "col4", "col5"])

        expected_result = spark.read.option("header", "true").csv(
            "./tests/test_set/test_rename_result_1.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_rename_missing_column(
        self, dataset_object: Data, spark: SparkSession
    ) -> None:
        """Testing Data.rename_columns() with missing one new colum name"""

        dataset_object.rename_columns(["col1", "col2", "col3", "col4"])

        expected_result = spark.read.option("header", "true").csv(
            "./tests/test_set/dataset_one.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_rename_too_many_new_columns(
        self, dataset_object: Data, spark: SparkSession
    ) -> None:
        """Testing Data.rename_columns() with too much given columns"""

        dataset_object.rename_columns(["col1", "col2", "col3", "col4", "col5", "col6"])

        expected_result = spark.read.option("header", "true").csv(
            "./tests/test_set/dataset_one.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)
