""" Test module of app.data.Data.drop_column() method"""
import chispa

from tests import SPARK
from app.data import Data


class TestDataDropColumn:
    """Testing class to test Data.drop_column() method"""

    def test_data_drop_single_column(self):
        """Testing Data.drop_column() dropping single column"""

        test_object = Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)

        test_object.drop_column(["last_name"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_drop_result_1.csv"
        )

        chispa.assert_df_equality(test_object.data, expected_result)

    def test_data_drop_with_ignoring_one(self):
        """
        Testing Data.drop_column() dropping single column with given 2
        (one need to be ignored)
        """

        test_object = Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)

        test_object.drop_column(["no_column", "last_name"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_drop_result_1.csv"
        )

        chispa.assert_df_equality(test_object.data, expected_result)

    def test_data_drop_more_columns(self):
        """Testing Data.drop_column() dropping more than one column"""

        test_object = Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)

        test_object.drop_column(["last_name", "first_name"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_drop_result_2.csv"
        )

        chispa.assert_df_equality(test_object.data, expected_result)
