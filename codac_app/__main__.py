#!/bin/python
"""
Codac assigment task
Data processing app with PySpark usage
author: Mateusz Jasnowski
github: github.com/mateuszjasnowski/pyspark_codac_assigment_task
"""
from argparse import ArgumentParser
import os

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


def app_start():
    """
    Catch given args and execute main().
    Required args:
    first_ds: str
    second_ds: str
    Optional args:
    -c, --country: str
    -m, --master: str
    -h, --help: None
    """

    # catching cmd arguments
    parser = ArgumentParser()
    parser.add_argument("first_ds", type=str, help="Path to first dataset")
    parser.add_argument("second_ds", type=str, help="Path to second dataset")

    parser.add_argument(
        "-c",
        "--country",
        action="append",
        help="Filter by countries (Can be used multiple times)",
    )

    parser.add_argument(
        "-m", "--master", type=str, help="Spark session's master address"
    )

    args = parser.parse_args()

    # catching master's address in env_var
    master = os.environ.get("SPARK_MASTER")

    # replacing master address if in app args
    if not master and not args.master:
        LOGGER.fatal("Cannot get env_var for SPARK_MASTER, nor given master argument")
        LOGGER.info("====================EOL====================")
        raise EnvironmentError(
            "Cannot get env_var for SPARK_MASTER, nor given master argument"
        )

    if args.master:
        master = args.master

    LOGGER.info("Recived %s arguments: %s", len(args.__dict__), args.__dict__)

    # calling app with given args
    if args.country:
        main(args.first_ds, args.second_ds, args.country, master_session=master)
    else:
        main(args.first_ds, args.second_ds, master_session=master)


if __name__ == "__main__":
    # Starts program if package have been called directly
    app_start()
