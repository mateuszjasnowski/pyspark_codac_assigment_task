""" Test module of app.data.Data.rename_columns() method"""
import chispa

from tests import SPARK
from app.data import Data


class TestDataRenameColumns:
    """Testing class to test Data.rename_columns() method"""

    def test_data_rename_correct_scenario(self):
        """Testing Data.rename_columns() successful renaming with given all needed column names"""

        test_object = Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)

        test_object.rename_columns(["col1", "col2", "col3", "col4", "col5"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_rename_result_1.csv"
        )

        chispa.assert_df_equality(test_object.data, expected_result)

    def test_data_rename_missing_column(self):
        """Testing Data.rename_columns() with missing one new colum name"""

        test_object = Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)

        test_object.rename_columns(["col1", "col2", "col3", "col4"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/dataset_one.csv"
        )

        chispa.assert_df_equality(test_object.data, expected_result)

    def test_data_rename_too_many_new_columns(self):
        """Testing Data.rename_columns() with too much given columns"""

        test_object = Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)

        test_object.rename_columns(["col1", "col2", "col3", "col4", "col5", "col6"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/dataset_one.csv"
        )

        chispa.assert_df_equality(test_object.data, expected_result)
