""" Test module of app.data.Data.join_data() method"""
import chispa
import pytest
from pyspark.sql import SparkSession

from codac_app.app.data import Data


class TestDataJoinData:
    """Testing class to test Data.join_data() method"""

    @pytest.fixture()
    def spark(self) -> SparkSession:
        """Declaring SparkSession for Chispa testing"""
        return SparkSession.builder.master("local").appName("ChispaTest").getOrCreate()

    @pytest.fixture()
    def first_dataset(self, spark: SparkSession) -> Data:
        """Creating Data object avalible for class's methods"""
        return Data(spark, "./tests/test_set/dataset_one.csv", header=True)

    @pytest.fixture()
    def second_dataset(self, spark: SparkSession) -> Data:
        """Creating Data object avalible for class's methods"""
        return Data(spark, "./tests/test_set/dataset_two.csv", header=True)

    def test_data_join_correct_join_order(
        self, first_dataset: Data, second_dataset: Data, spark: SparkSession
    ) -> None:
        """Testing join_data() method joining 2 data sets in correct order"""

        first_dataset.join_data(
            second_dataset.data,
            second_dataset.data_frame_name,
            first_pk="id", second_pk="id"
        )

        expected_result = spark.read.option("header", "true").csv(
            "./tests/test_set/test_join_result_1.csv"
        )

        chispa.assert_df_equality(first_dataset.data, expected_result)

    def test_data_join_wrong_order(
        self,
        first_dataset: Data,
        second_dataset: Data,
        spark: SparkSession) -> None:
        """Testing join_data() method joining 2 data sets in wrong order"""


        second_dataset.join_data(
            first_dataset.data,
            first_dataset.data_frame_name,
            first_pk="id", second_pk="id"
        )

        expected_result = spark.read.option("header", "true").csv(
            "./tests/test_set/test_join_result_2.csv"
        )

        chispa.assert_df_equality(second_dataset.data, expected_result)
