""" Test module of app.data.Data.__init__ method"""
import pytest
from pyspark.sql.utils import AnalysisException

from codac_app.app.data import Data
from . import SPARK


class TestDataInit:
    """Testing class to test Data.__init__ method"""

    @pytest.fixture()
    def dataset_object(self):
        """Creating Data object avalible for class's methods"""
        return Data(SPARK, "./tests/test_set/dataset_one.csv", header=True)

    def test_data_init_correct_scenario(self, dataset_object):
        """
        Testing __init__ method
        creating Data with correct values
        """

        columns = dataset_object.data.columns

        assert dataset_object.data_frame_name == "dataset_one.csv"
        assert dataset_object.data.count() == 240
        assert len(columns) == 5
        assert columns == ["id", "first_name", "last_name", "email", "country"]

    def test_data_init_correct_scenario_no_header(self):
        """
        Testing __init__ method
        creating Data with correct values without reading file's header
        """

        test_object = Data(SPARK, "./tests/test_set/dataset_one.csv", header=False)

        columns = test_object.data.columns

        assert test_object.data_frame_name == "dataset_one.csv"
        assert test_object.data.count() == 241
        assert len(columns) == 5
        assert columns == ["_c0", "_c1", "_c2", "_c3", "_c4"]

    def test_data_init_not_existing_file(self):
        """Testing __init__ behaviour if given not existing file"""

        with pytest.raises(AnalysisException):
            Data(SPARK, "./tests/test_set/dataset_one2.csv", header=False)
