""" Test module of app.data.Data.filter method"""
import chispa

from tests import SPARK
from app.data import Data

class TestDataFilter():
    """ Testing class to test Data.filter() method """

    def test_data_filter_one_country(self):
        """ Testing .filter() method by filtering by one country """

        test_object = Data(
            SPARK,
            "./tests/test_set/dataset_one.csv",
            header=True
            )
        test_object.filter("country", ["Netherlands"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_filter_result_1.csv"
            )

        chispa.assert_df_equality(test_object.data, expected_result)
