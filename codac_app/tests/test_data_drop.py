""" Test module of app.data.Data.drop_column() method"""
import chispa
import pytest

from . import SPARK
from codac_app.app.data import Data


class TestDataDropColumn:
    """Testing class to test Data.drop_column() method"""

    @pytest.fixture()
    def dataset_object(self):
        """Creating Data object avalible for class's methods"""
        return Data(SPARK, "./codac_app/tests/test_set/dataset_one.csv", header=True)

    def test_data_drop_single_column(self, dataset_object):
        """Testing Data.drop_column() dropping single column"""

        dataset_object.drop_column(["last_name"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./codac_app/tests/test_set/test_drop_result_1.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_drop_with_ignoring_one(self, dataset_object):
        """
        Testing Data.drop_column() dropping single column with given 2
        (one need to be ignored)
        """

        dataset_object.drop_column(["no_column", "last_name"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./codac_app/tests/test_set/test_drop_result_1.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_drop_more_columns(self, dataset_object):
        """Testing Data.drop_column() dropping more than one column"""

        dataset_object.drop_column(["last_name", "first_name"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./codac_app/tests/test_set/test_drop_result_2.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)
