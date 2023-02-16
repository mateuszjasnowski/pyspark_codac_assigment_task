"""
Codac assigment task
Data processing app with PySpark usage
author: Mateusz Jasnowski
github: github.com/mateuszjasnowski/pyspark_codac_assigment_task
"""
from argparse import ArgumentParser
import json

from app.cm_app_session import AppSession
from app.data import Data
from app import LOGGER


def run_instruction(
    session: dict,
    do_open: dict,
    do_transform: dict = None,
    do_join: dict = None,
    do_save: dict = None,
) -> None:
    """
    Executes instuctions
    - Start or connect to spark's session
    - Open client_data and clients_cards files (Details is docs)
    - execute "transform" instructions
    - execute "join" instructions
    - execute "save" instructions
    """
    LOGGER.info("==== Starting instructions executions ====")
    LOGGER.info("Config: %s", session)
    LOGGER.info("Open: %s", do_open)
    LOGGER.info("Transform: %s", do_transform)
    LOGGER.info("Join: %s", do_join)
    LOGGER.info("Save: %s", do_save)
    LOGGER.info("=================EXECUTING=================")

    with AppSession(master=session["master"], app_name=session["name"]) as sp_session:
        client_data = Data(
            sp_session,
            do_open["client_data"]["path"],
            header=do_open["client_data"]["header"],
        )
        clients_cards = Data(
            sp_session,
            do_open["clients_cards"]["path"],
            header=do_open["clients_cards"]["header"],
        )

        if do_transform:
            # apply filters
            for filter_key, filter_val in do_transform["filter"].items():
                client_data.filter(filter_key, filter_val)
                clients_cards.filter(filter_key, filter_val)

            # dropping columns
            if "drop_columns" in do_transform:
                client_data.drop_column(do_transform["drop_columns"])
                clients_cards.drop_column(do_transform["drop_columns"])

            # renaming columns
            for column in do_transform["rename_columns"]:
                client_data.rename(column["from"], column["to"])
                clients_cards.rename(column["from"], column["to"])

        if do_join:
            client_data.join_data(
                clients_cards.data,
                clients_cards.data_frame_name,
                do_join["master"],
                do_join["joining"],
            )

        if do_save:
            client_data.save(
                do_save["file_type"], do_save["path"], header=do_save["header"]
            )


def main() -> None:
    """
    App read .json instruction file and execute data processing.
    App's running steps
    1. Start app
    2. Read passed arguments (--help for details)
    3. Open instruction file
    4. Start executing the instruction
    - Start or connect to spark's session
    - Open client_data and clients_cards files (Details is docs)
    - Look for "transform" instructions and execute
    - Look for "join" instructions and execute
    - Look for "save" instructions and execute
    """

    # Read arguments
    parser = ArgumentParser()
    parser.add_argument(
        "instruction_file", type=str, help="path to instruction file (.json)"
    )
    parser.add_argument("--client_data", type=str, help="client_data file path")
    parser.add_argument("--clients_cards", type=str, help="clients_cards file path")
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

    # Reading .json file
    with open(args.instruction_file, "r", encoding="utf-8") as instructions:
        actions = json.load(instructions)

    # Execute instruction file
    if actions["config"]:
        session_config = actions["config"]

    # Get sessions master address
    # Argument -m (--master) have priority before json file
    if args.master:
        session_config = {"master": args.master, "name": "Local codac app"}

    if not actions["config"] and not args.master:
        raise ValueError(
            "Missing master pyspark session address.\n"
            + "Add config details in manual json file or\n"
            + "pass master's address as app's argument (--help for details)\n"
            + "For details check instruction file's documentation."
        )

    if actions["open"]:
        open_actions = actions["open"]

    if args.client_data and args.clients_cards:
        open_actions = {
            "client_data": {"path": args.client_data, "header": "true"},
            "clients_cards": {"path": args.clients_cards, "header": "true"},
        }

    if not actions["open"] and not args.client_data and not args.clients_cards:
        raise ValueError(
            "Missing client_data or/and clients_cards!\n"
            + "Add object/s files in manual json file or\n"
            + "pass files path as app's argument (--help for details)\n"
            + "For details check instruction file's documentation."
        )

    transform_action = None
    if "transform" in actions:
        transform_action = actions["transform"]

    join_actions = None
    if "join" in actions:
        join_actions = actions["join"]

    save_actions = None
    if "save" in actions:
        save_actions = actions["save"]

    run_instruction(
        session_config,
        open_actions,
        transform_action,
        join_actions,
        save_actions,
    )


if __name__ == "__main__":
    # Starts program if package have been called directly
    main()
