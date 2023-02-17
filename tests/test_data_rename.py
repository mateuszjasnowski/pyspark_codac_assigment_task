""" Test module of app.data.Data.rename_columns() method"""
import chispa
import pytest
from pyspark.sql import SparkSession

from codac_app.app.data import Data


class TestDataRenameAllColumns:
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


class TestDataRenameColumn:
    """Testing class to test Data.rename() method"""

    @pytest.fixture()
    def spark(self) -> SparkSession:
        """Declaring SparkSession for Chispa testing"""
        return SparkSession.builder.master("local").appName("ChispaTest").getOrCreate()

    @pytest.fixture()
    def dataset_object(self, spark: SparkSession) -> Data:
        """Setting up Data object for test"""
        return Data(spark, "./tests/test_set/dataset_one.csv", header=True)

    def test_data_rename_single_col(
        self, dataset_object: Data, spark: SparkSession
    ) -> None:
        """Testing Data.rename() by renaming single column at begining of table"""

        dataset_object.rename("id", "col1")

        expected_result = spark.read.option("header", "true").csv(
            "./tests/test_set/test_rename_result_2.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_rename_single_col_last(
        self, dataset_object: Data, spark: SparkSession
    ) -> None:
        """Testing Data.rename() by renaming single column at end of table"""

        dataset_object.rename("country", "col1")

        expected_result = spark.read.option("header", "true").csv(
            "./tests/test_set/test_rename_result_3.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_rename_single_col_middle(
        self, dataset_object: Data, spark: SparkSession
    ) -> None:
        """Testing Data.rename() by renaming single column in the middle of table"""

        dataset_object.rename("last_name", "col1")

        expected_result = spark.read.option("header", "true").csv(
            "./tests/test_set/test_rename_result_4.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_rename_multiple_cols(
        self, dataset_object: Data, spark: SparkSession
    ) -> None:
        """Testing Data.rename() by renaming multiple cols"""

        dataset_object.rename("id", "col1")
        dataset_object.rename("last_name", "col2")
        dataset_object.rename("email", "col3")

        expected_result = spark.read.option("header", "true").csv(
            "./tests/test_set/test_rename_result_5.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_rename_not_column_found(
        self, dataset_object: Data, spark: SparkSession
    ) -> None:
        """Testing Data.rename() by giving not existing column name"""

        dataset_object.rename("client_id", "col1")

        expected_result = spark.read.option("header", "true").csv(
            "./tests/test_set/dataset_one.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_rename_single_column_with_empty(
        self, dataset_object: Data, spark: SparkSession
    ) -> None:
        """
        Testing Data.rename() by renaming single column
        with giving not existing column name as well
        """

        dataset_object.rename("id", "col1")
        dataset_object.rename("client_id", "col1")

        expected_result = spark.read.option("header", "true").csv(
            "./tests/test_set/test_rename_result_2.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)
