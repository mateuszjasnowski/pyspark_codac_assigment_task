#!/bin/python
"""
Codac assigment task
Data processing app with PySpark usage
author: Mateusz Jasnowski
github: github.com/mateuszjasnowski/pyspark_codac_assigment_task
"""

from codac_app.app import LOGGER, APP_CONFIG
from codac_app.app.data import Data
from codac_app.app.cm_app_session import AppSession


def main(
    first_dataset: str,
    second_dataset: str,
    country: list = None,
    master_session: str = None,
) -> None:
    """App inizialization and performing actions"""
    LOGGER.debug("%s %s %s", first_dataset, second_dataset, country)

    with AppSession(master=master_session) as sp_session:
        dataframe = Data(sp_session, first_dataset)
        if country:
            dataframe.filter("country", country)

        dataframe.join_data(second_dataset)
        dataframe.drop_column(APP_CONFIG["columns_to_drop"])

        dataframe.rename_columns(APP_CONFIG["expected_column_names"])
        dataframe.save()
