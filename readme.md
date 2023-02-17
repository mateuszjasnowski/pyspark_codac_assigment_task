# PySpark Codac Assigment Task
###### *by Mateusz Jasnowski*
---
# Task background:
A very small company called **KommatiPara** that deals with bitcoin trading has two separate datasets dealing with clients that they want to collate to starting interfacing more with their clients. One dataset contains information about the clients and the other one contains information about their financial details.

The company now needs a dataset containing the emails of the clients from the United Kingdom and the Netherlands and some of their financial details to starting reaching out to them for a new marketing push.

Since all the data in the datasets is fake and this is just an exercise, one can forego the issue of having the data stored along with the code in a code repository.
## App requriments:
- [x] Use Python **3.7**
- [x] Avoid using notebooks, like **Jupyter** for instance.
- [x] Only use clients from the **United Kingdom** or the **Netherlands**.
- [x] Remove personal identifiable information from the first dataset, **excluding emails**.
- [x] Remove credit card number from the second dataset.
- [x] Data should be joined using the **id** field.
- [x] Rename the columns for the easier readability to the business users:

|Old name|New name|
|--|--|
|id|client_identifier|
|btc_a|bitcoin_address|
|cc_t|credit_card_type|

- [x] Output files should be located in a **client_data** directory in the root directory of the project.
- [x] Application should receive three arguments, the paths to each of the dataset files and also the countries to filter as the client wants to reuse the code for other countries.
- [x] Generic functions for filtering data and renaming.
- [x] App create **logs**.
- [x] App allows for filtering data and renaming.
- [x] Testing implementing chispa package *https://github.com/MrPowers/chispa)*
- [x] *(Optional)* Automated build pipeline. [^1]
- [x] *(Optional)* Log to a file with a rotating policy. [^1]
- [x] *(Optional)* Code should be able to be packaged into a source distribution file. [^1]

[^1]: Addtional requriment not fully need to be fullfilled
---
# Application

## Setup and run

Manual package build
```python
python setup.py bdist_wheel
```

Install downloaded or builded .whl package
```python
pip install CodacApp-[...].whl
```

Run app with command:
```python
codac-app [-h] [--client_data CLIENT_DATA]
               [--clients_cards CLIENTS_CARDS] [-c COUNTRY] [-m MASTER]
               instruction_file
```

App is reciving arguments in command line.

**Required arguments**
Argument name | Type | Description
--- | --- | ---
instruction_file | string | Path to instruction file (.json). Details can be found in docs.

**Instruction json file**

Instructions file is required to run an app. Contains execution instruction for app to open, transform and save file. Example file and documentation can be found at [exec_instruction](exec_instuction/readme.md)

**Not required arguments**
Argument name | Type | Description
--- | --- | ---
-h (--help) | none | Show this help message and exit
--client_data | string | Path to client_data file. Argument will replace given client_data path in instruction file.
--clients_cards | string | Path to clients_cards file. Argument will replace given clients_cards path in instruction file.
-c (--country) | string | Filtering data by given country (Can be multiple times)
-m (--master) | string | Spark session's master address. Argument will replace given master address in instruction file.

**Data set files**

Application is reciving two ```.csv``` files with specific schema.

***1. client_data dataset***
column name | data type | description
--- | --- | ---
id | integer | identification number of client
first_name | string | first name of client
last_name | string | last name of client
email | string | client's email
country | string | client's country

***2. clients_cards dataset***
column name | data type | description
--- | --- | ---
id | integer | identification number of client
btc_a | string | bitcoin address
cc_t | string | credit card type
cc_n | string | credit card number

## Output

Application is processing given data sets and output file at location ```./client_data/``` with following schema (at default):
client_identifier | email | country | bitcoin_address | credit_card_type
--- | --- | --- | --- | ---
integer | string | string | string | string
