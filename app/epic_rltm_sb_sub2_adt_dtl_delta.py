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

# MAGIC %run ../code/dtl/dtl_delta_subscriber

# COMMAND ----------

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# COMMAND ----------

SUBSCRIPTION_NAME = "parsed_hl7_adt_to_delta_dtl"

# COMMAND ----------

detail_table_locs = {
  "adt_dtl" : "{}/raw/rltm/epic/adt_dtl".format(os.getenv("adls_file_path")),
  "encntr_dx" : "{}/raw/rltm/epic/encntr_dx".format(os.getenv("adls_file_path")),
  "encntr_er_complnt" : "{}/raw/rltm/epic/encntr_er_complnt".format(os.getenv("adls_file_path")),
  "encntr_visit_rsn" : "{}/raw/rltm/epic/encntr_visit_rsn".format(os.getenv("adls_file_path")),
  "encntr_nte" : "{}/raw/rltm/epic/encntr_nte".format(os.getenv("adls_file_path"))
}

detail_table_names = {
  "adt_dtl" : "epic_rltm.adt_dtl",
  "encntr_dx" : "epic_rltm.encntr_dx",
  "encntr_er_complnt" : "epic_rltm.encntr_er_complnt",
  "encntr_visit_rsn" : "epic_rltm.encntr_visit_rsn",
  "encntr_nte": "epic_rltm.encntr_nte"
}

staging_table_name = "epic_rltm_stg.adt_dtl_stg"

# COMMAND ----------

persist_dtl_data_from_subscription_with_staging(service_bus_topic_receive_conn_str,service_bus_topic_name,SUBSCRIPTION_NAME,detail_table_locs,detail_table_names,staging_table_name)