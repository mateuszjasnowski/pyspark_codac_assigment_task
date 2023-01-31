"""
Codac assigment task
Data processing app with PySpark usage
author: Mateusz Jasnowski
github: github.com/mateuszjasnowski/pyspark_codac_assigment_task
"""
from app import LOGGER

def main(first_dataset: str, second_dataset: str, filter: dict={}):
    pass

if __name__ == "__main__":
    """
    Start program if package have been called directly
    Catch given args and execute main().
    """
    LOGGER.debug("Test warning")

    #TODO catch args
    first_file = 'source_data\dataset_one.csv'
    second_file = 'source_data\dataset_two.csv'

    LOGGER.debug(f"Frist file: {first_file}")
    LOGGER.debug(f"Second file: {second_file}")
