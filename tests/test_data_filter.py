""" Test module of app.data.Data.filter method"""
import chispa
import pytest
from pyspark.sql.utils import AnalysisException

from tests import SPARK
from app.data import Data


class TestDataFilter:
    """Testing class to test Data.filter() method"""

    def test_data_filter_one_country(self):
        """Testing .filter() method by filtering by one country"""

        test_object = Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)
        test_object.filter("country", ["Netherlands"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_filter_result_1.csv"
        )

        chispa.assert_df_equality(test_object.data, expected_result)

    def test_data_filter_more_countries(self):
        """Testing .filter() method by filtering by more than one country"""

        test_object = Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)
        test_object.filter("country", ["Netherlands", "United States"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_filter_result_2.csv"
        )

        chispa.assert_df_equality(test_object.data, expected_result)

    def test_data_filter_empty_result(self):
        """Testing .filter() method by filtering by country not existing in data"""

        test_object = Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)
        test_object.filter("country", ["Poland"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_filter_result_3.csv"
        )

        chispa.assert_df_equality(test_object.data, expected_result)

    def test_data_filter_empty_result_with_one(self):
        """
        Testing .filter() method by filtering by country not existing in data
        and addtional existing one
        """

        test_object = Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)
        test_object.filter("country", ["Poland", "Netherlands"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_filter_result_1.csv"
        )

        chispa.assert_df_equality(test_object.data, expected_result)

    def test_data_filter_id_column_filter(self):
        """Testing .filter() method by filtering by id"""

        test_object = Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)
        test_object.filter("id", ["3", "150", "200"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_filter_result_4.csv"
        )

        chispa.assert_df_equality(test_object.data, expected_result)

    def test_data_filter_name(self):
        """Testin .filter() method by filtering by first_name"""

        expected_data = [
            ("16", "Callie", "d'Arcy", "cdarcyf@people.com.cn", "United States"),
            ("104", "Holly-anne", "Ostler", "hostler2v@aboutads.info", "France"),
        ]
        expected_result = SPARK.createDataFrame(
            expected_data, ["id", "first_name", "last_name", "email", "country"]
        )

        test_object = Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)
        test_object.filter("first_name", ["Holly-anne", "Callie"])

        chispa.assert_df_equality(test_object.data, expected_result)

    def test_data_filter_not_existing_column(self):
        """Testin .filter() method by filtering by not existing column name"""
        test_object = Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)

        with pytest.raises(AnalysisException):
            test_object.filter("name", ["Holly-anne", "Callie"])
