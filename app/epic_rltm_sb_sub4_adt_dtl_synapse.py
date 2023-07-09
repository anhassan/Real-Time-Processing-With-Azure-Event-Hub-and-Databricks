# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook reads messages from Service Bus Topic and write data to Synapse ADT Hist Tables
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 09/01/2022     AHASSAN2          Initial creation
# 03/27/2023     AHASSAN2          Added staging layer logic

# COMMAND ----------

!pip install uamqp

# COMMAND ----------

# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

# MAGIC %run ../code/dtl/dtl_synapse_subscriber

# COMMAND ----------

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

SUBSCRIPTION_NAME = "parsed_hl7_adt_to_synapse_dtl"

# COMMAND ----------

detail_table_names = {
  "adt_dtl" : "epic_rltm.adt_dtl",
  "encntr_dx" : "epic_rltm.encntr_dx",
  "encntr_er_complnt" : "epic_rltm.encntr_er_complnt",
  "encntr_visit_rsn" : "epic_rltm.encntr_visit_rsn",
  "encntr_nte" : "epic_rltm.encntr_nte"
}

staging_table_name = "epic_rltm_stg.adt_dtl_synapse_stg"

# COMMAND ----------

persist_dtl_data_from_subscription_synapse_with_staging(service_bus_topic_receive_conn_str, service_bus_topic_name, SUBSCRIPTION_NAME, detail_table_names, staging_table_name)

# COMMAND ----------

