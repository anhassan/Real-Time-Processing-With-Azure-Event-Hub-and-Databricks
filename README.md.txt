# Design and Implementation

Developed Spark streaming processes in Databricks to read HL7 messages from Azure Service Bus, populated from an on-premise Tibco Queue, parse them and eventually persist them in Delta tables and Synapse Dedicated SQL Pool. Not all the data persisted in the tables insert only, some of the tables only contain the latest data for each id (primary key) by implementation of Change Data Capture (CDC) logic through MERGE in Delta tables and INSERT, DELETE in Synapse Dedicated SQL Pool tables

***Tools Used*** : Azure Data Lake Storage, Azure Event Hub , Azure Service Bus, Azure Databricks, Azure KeyVault, Azure Synapse Analytics

***Target Domains*** : Data Ingestion, Data Transformation, Data Storage

The following architecture diagram explains the entire ETL real time pipeline in detail

<p align="center">
  <img src="/assets/rtd_databricks_eventhub_architecture.jpg" />
</p>