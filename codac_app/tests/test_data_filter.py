""" Test module of app.data.Data.filter method"""
import chispa
import pytest
from pyspark.sql.utils import AnalysisException

from codac_app.app.data import Data
from . import SPARK


class TestDataFilter:
    """Testing class to test Data.filter() method"""

    @pytest.fixture()
    def dataset_object(self):
        """Creating Data object avalible for class's methods"""
        return Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)

    def test_data_filter_one_country(self, dataset_object):
        """Testing .filter() method by filtering by one country"""

        dataset_object.filter("country", ["Netherlands"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_filter_result_1.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_filter_more_countries(self, dataset_object):
        """Testing .filter() method by filtering by more than one country"""

        dataset_object.filter("country", ["Netherlands", "United States"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_filter_result_2.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_filter_empty_result(self, dataset_object):
        """Testing .filter() method by filtering by country not existing in data"""

        dataset_object.filter("country", ["Poland"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_filter_result_3.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_filter_empty_result_with_one(self, dataset_object):
        """
        Testing .filter() method by filtering by country not existing in data
        and addtional existing one
        """

        dataset_object.filter("country", ["Poland", "Netherlands"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_filter_result_1.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_filter_id_column_filter(self, dataset_object):
        """Testing .filter() method by filtering by id"""

        dataset_object.filter("id", ["3", "150", "200"])

        expected_result = SPARK.read.option("header", "true").csv(
            "./tests/test_set/test_filter_result_4.csv"
        )

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_filter_name(self, dataset_object):
        """Testin .filter() method by filtering by first_name"""

        expected_data = [
            ("16", "Callie", "d'Arcy", "cdarcyf@people.com.cn", "United States"),
            ("104", "Holly-anne", "Ostler", "hostler2v@aboutads.info", "France"),
        ]
        expected_result = SPARK.createDataFrame(
            expected_data, ["id", "first_name", "last_name", "email", "country"]
        )

        dataset_object.filter("first_name", ["Holly-anne", "Callie"])

        chispa.assert_df_equality(dataset_object.data, expected_result)

    def test_data_filter_not_existing_column(self, dataset_object):
        """Testin .filter() method by filtering by not existing column name"""

        with pytest.raises(AnalysisException):
            dataset_object.filter("name", ["Holly-anne", "Callie"])
