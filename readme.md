# PySpark Codac Assigment Task
###### *by Mateusz Jasnowski*
---
## Task background:
A very small company called **KommatiPara** that deals with bitcoin trading has two separate datasets dealing with clients that they want to collate to starting interfacing more with their clients. One dataset contains information about the clients and the other one contains information about their financial details.

The company now needs a dataset containing the emails of the clients from the United Kingdom and the Netherlands and some of their financial details to starting reaching out to them for a new marketing push.

Since all the data in the datasets is fake and this is just an exercise, one can forego the issue of having the data stored along with the code in a code repository.
## App requriments:
- Use Python **3.7**
- Avoid using notebooks, like **Jupyter** for instance.
- Only use clients from the **United Kingdom** or the **Netherlands**.
- Remove personal identifiable information from the first dataset, **excluding emails**.
- Remove credit card number from the second dataset.
- Data should be joined using the **id** field.
- Rename the columns for the easier readability to the business users:

|Old name|New name|
|--|--|
|id|client_identifier|
|btc_a|bitcoin_address|
|cc_t|credit_card_type|

- Output files should be located in a **client_data** directory in the root directory of the project.
- Application should receive three arguments, the paths to each of the dataset files and also the countries to filter as the client wants to reuse the code for other countries.
- App create **logs**.
- App allows for filtering data and renaming.
- *Automated build pipeline.
- *Log to a file with a rotating policy.
- *Code should be able to be packaged into a source distribution file.

### *Addtional requriment not fully need to be fullfilled
---
## App setup
**TO DO**

## App run

**TO DO**

## Placeholder

**TO DO**

