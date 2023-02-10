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
- [ ] Testing implementing chispa package *https://github.com/MrPowers/chispa)*
- [ ] *(Optional)* Automated build pipeline. [^1]
- [x] *(Optional)* Log to a file with a rotating policy. [^1]
- [ ] *(Optional)* Code should be able to be packaged into a source distribution file. [^1]

[^1]: Addtional requriment not fully need to be fullfilled
---
# Application
## App setup
**TO DO**

**Config file**

App can be configured by editing ```app_config.json```

File's content:
```json
{
    "columns_to_drop": [
        # Names of columns to be dropped form joined data set
        # Usefull if source data sets contains secret data
    ],
    "expected_column_names": [
        # Names of columns in output data set
        # IMPORTANT ! Give the same amount of columns as
        # will exists in output file
    ]
}
```


## App run

Run app with command:
```python
python main.py [-h] [-c COUNTRY] [-m MASTER] first_ds second_ds
```

App is reciving arguments in command line.

**Required arguments**
Argument name | Type | Description
--- | --- | ---
first_ds | string | Path to first data set file
second_ds | string | Path to second data set file

**Not required arguments**
Argument name | Type | Description
--- | --- | ---
-c (--country) | string | Filtering data by given country (Can be multiple times)
-m (--master) | string | Spark session's master address
-h (--help) | empty | Display help menu

**Data set files**

Application is reciving two ```csv``` files with specific schema.

***First dataset***
column name | data type | description
--- | --- | ---
id | integer | identification number of client
first_name | string | first name of client
last_name | string | last name of client
email | string | client's email
country | string | client's country

***Second dataset***
column name | data type | description
--- | --- | ---
id | integr | identification number of client
btc_a | string | bitcoin address
cc_t | string | credit card type
cc_n | string | credit card number

## Placeholder

**TO DO**

