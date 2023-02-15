""" Test module of app.data.Data.join_data() method"""
import chispa
import pytest

from codac_app.app.data import Data
from . import SPARK


class TestDataJoinData:
    """Testing class to test Data.join_data() method"""

    @pytest.fixture()
    def dataset_object(self):
        """Creating Data object avalible for class's methods"""
        return Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)

    def test_data_join_correct_join_order(self, dataset_object):
        """Testing join_data() method joining 2 data sets in correct order"""

        dataset_object.join_data(
            "./tests/test_set/dataset_two.csv", first_pk="id", second_pk="id"
        )

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_join_result_1.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_join_wrong_order(self):
        """Testing join_data() method joining 2 data sets in wrong order"""

        test_object = Data(SPARK, "./tests/test_set/dataset_two.csv", header=True)

        test_object.join_data(
            "./tests/test_set/dataset_one.csv", first_pk="id", second_pk="id"
        )

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_join_result_2.csv"
        )

        chispa.assert_df_equality(test_object.data, expected_result)
