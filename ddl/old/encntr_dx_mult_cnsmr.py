# Databricks notebook source
import os

# COMMAND ----------

dbutils.fs.rm('{}/raw/epic_rltm/encntr_dx_mult_cnsmr'.format(os.getenv("adls_file_path")),True)

# COMMAND ----------

drop_table = "drop table if exists epic_rltm.encntr_dx_mult_cnsmr"

create_table = """ 
CREATE TABLE epic_rltm.encntr_dx_mult_cnsmr (
  msg_src STRING NOT NULL,
  msg_tm TIMESTAMP,
  pat_enc_csn_id DECIMAL(18,0) NOT NULL,
  dx_icd_cd STRING,
  dx_name STRING,
  dx_code_type STRING,
  row_insert_tsp TIMESTAMP,
  row_updt_tsp TIMESTAMP,
  insert_user_id STRING,
  update_user_id STRING)
USING delta
LOCATION '{}/raw/epic_rltm/encntr_dx_mult_cnsmr'""".format(os.getenv("adls_file_path"))

spark.sql(drop_table)
spark.sql(create_table)