# lkc-sapgui-flow

This solution aims to ingest data from production SAP4HANA application using SAPGUI, perform simple transformations and load it to a raw/bronze data layer in SQL Server and AWS S3.

It is also capable of getting all data from very heavy sap table like BSEG by looping through multiple ranges of dates and appending all data into a single dataframe.

It can be useful for some situation where you cant actually access SAP data through better ways like an SAP API or SAP BTP solutions.

This can be deployed to any instance that runs on windows and have SAPGUI already installed. Must be run as module.