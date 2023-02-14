#!/bin/python
"""
Codac assigment task
Data processing app with PySpark usage
author: Mateusz Jasnowski
github: github.com/mateuszjasnowski/pyspark_codac_assigment_task
"""
from argparse import ArgumentParser
import os

from codac_app import main
from codac_app.app import LOGGER

if __name__ == "__main__":
    # Starts program if package have been called directly
    # Catch given args and execute main().
    # Required args:
    # first_ds: str
    # second_ds: str
    # Optional args:
    # -c, --country: str
    # -m, --master: str
    # -h, --help: None

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
