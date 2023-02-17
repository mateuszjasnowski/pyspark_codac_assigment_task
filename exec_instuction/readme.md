# Instruction file documentation
App requires as an argument to give path to ```.json``` manual file.
File gives information about steps to be executed.

**Actions groups:**
- open - Open dataset file
- transform - Make transformation of files (rename columns, drop columns, filter data)
- join - Join two DataFrames
- save - Save DataFram to file with included transformations

**App configuration group**
- master - Contains Spark session's address
- app name - Contains app name

## File schema

```python
{
    "config": {
        "master": str, # Spark session's address
        "name": str # App's name (will be visible at Spark master)
    },
    "open": { # Required part if not send as cmd argument
        "client_data": {
            "path": str, # Path to client_data file
            "header": str # "true" or "false"
        },
        "clients_cards": {
            "path": str, # Path to clients_cards file
            "header": str # "true" or "false"
        }
    },
    "transform": {
        "filter": {
            "column name": [ # Name of column to be filtered
                "value_1", # Data to match in filtered column
                "[...]",
                "value_n"
            ],
            #Can be used more than one time
            #[...]
        },
        "rename_columns": [
            {
                "from": str, # Old column name
                "to": str # New column name
            },
            #Can be used more than one time
            #[...]
            {
                "from": "old_column",
                "to": "new_column"
            }
        ],
        "drop_columns": [
            "column name", #name/s of columns to be dropped
            #[...]
        ]
    },
    "join": {
        "master": str, # Join column name from client_data
        "joining": str # Join column name from clients_cards
    },
    "save": {
        "path": str, # Path to subfoder where file will be saved
        "file_type": str, # "csv" or "parquet"
        "header": str, # "true" or "false"
        "mode": "overwrite" # Spark DataFrame save mode
    }
}
```