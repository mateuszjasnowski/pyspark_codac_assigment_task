#!/bin/python
"""
Codac assigment task
Data processing app with PySpark usage
author: Mateusz Jasnowski
github: github.com/mateuszjasnowski/pyspark_codac_assigment_task
"""
from argparse import ArgumentParser

from app import LOGGER


def main(first_dataset: str, second_dataset: str, country=list):
    LOGGER.debug("%s %s %s", first_dataset, second_dataset, country)


if __name__ == "__main__":
    # Start program if package have been called directly
    # Catch given args and execute main().
    # Required args:
    # first_ds: str
    # second_ds: str
    # Optional args:
    # -c, --country: str
    # -h, --help: None

    parser = ArgumentParser()
    parser.add_argument("first_ds", type=str, help="Path to first dataset")
    parser.add_argument("second_ds", type=str, help="Path to second dataset")

    parser.add_argument(
        "-c",
        "--country",
        action="append",
        help="Filter by countries (Can be used multiple times)",
    )

    args = parser.parse_args()

    LOGGER.info("Recived %s arguments: %s", len(args.__dict__), args.__dict__)

    if args.country:
        main(args.first_ds, args.second_ds, args.country)
    else:
        main(args.first_ds, args.second_ds)
