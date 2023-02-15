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
    def dataset_object(self, spark: SparkSession) -> Data:
        """Creating Data object avalible for class's methods"""
        return Data(spark, "./tests/test_set/dataset_one.csv", header=True)

    def test_data_join_correct_join_order(
        self, dataset_object: Data, spark: SparkSession
    ) -> None:
        """Testing join_data() method joining 2 data sets in correct order"""

        dataset_object.join_data(
            "./tests/test_set/dataset_two.csv", first_pk="id", second_pk="id"
        )

        expected_result = spark.read.option("header", "true").csv(
            "./tests/test_set/test_join_result_1.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_join_wrong_order(self, spark: SparkSession) -> None:
        """Testing join_data() method joining 2 data sets in wrong order"""

        test_object = Data(spark, "./tests/test_set/dataset_two.csv", header=True)

        test_object.join_data(
            "./tests/test_set/dataset_one.csv", first_pk="id", second_pk="id"
        )

        expected_result = spark.read.option("header", "true").csv(
            "./tests/test_set/test_join_result_2.csv"
        )

        chispa.assert_df_equality(test_object.data, expected_result)
