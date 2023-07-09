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
  "adt_dtl" : "{}/curated/epic_rltm/adt_dtl_mult_cnsmr".format(os.getenv("adls_file_path")),
  "encntr_dx" : "{}/curated/epic_rltm/encntr_dx_mult_cnsmr".format(os.getenv("adls_file_path")),
  "encntr_er_complnt" : "{}/curated/epic_rltm/encntr_er_complnt_mult_cnsmr".format(os.getenv("adls_file_path")),
  "encntr_visit_rsn" : "{}/curated/epic_rltm/encntr_visit_rsn_mult_cnsmr".format(os.getenv("adls_file_path"))
}

detail_table_names = {
  "adt_dtl" : "epic_rltm.adt_dtl_mult_cnsmr",
  "encntr_dx" : "epic_rltm.encntr_dx_mult_cnsmr",
  "encntr_er_complnt" : "epic_rltm.encntr_er_complnt_mult_cnsmr",
  "encntr_visit_rsn" : "epic_rltm.encntr_visit_rsn_mult_cnsmr"
}

staging_table_name = "epic_rltm.adt_dtl_mult_cnsmr_staging"

# COMMAND ----------

persist_dtl_data_from_subscription_with_staging(service_bus_topic_receive_conn_str,service_bus_topic_name,SUBSCRIPTION_NAME,detail_table_locs,detail_table_names,staging_table_name)