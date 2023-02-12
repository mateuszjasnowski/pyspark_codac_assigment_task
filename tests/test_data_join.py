""" Test module of app.data.Data.join_data() method"""
import chispa

from tests import SPARK
from app.data import Data

class TestDataInit():
    """ Testing class to test Data.join_data() method """

    def test_data_join_correct_join_order(self):
        """ Testing join_data() method """

        test_object = Data(
            SPARK,
            "./tests/test_set/dataset_one.csv",
            header=True
        )

        test_object.join_data(
            "./tests/test_set/dataset_one.csv",
            first_pk="id",
            second_pk="id"
            )

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_filter_result_5.csv"
            )

        chispa.assert_df_equality(test_object.data, expected_result)


