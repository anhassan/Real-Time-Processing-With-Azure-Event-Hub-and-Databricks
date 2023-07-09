# Databricks notebook source
import os

# COMMAND ----------

dbutils.fs.rm('{}/raw/rltm/epic/flwsht'.format(os.getenv("adls_file_path")),True)

# COMMAND ----------

drop_table = "DROP TABLE IF EXISTS epic_rltm.flwsht"

create_table = """
CREATE TABLE epic_rltm.flwsht (
  msg_typ STRING,
  msg_tm TIMESTAMP,
  msg_nm STRING,
  pat_mrn_id STRING,
  pat_enc_csn_id DECIMAL(18,0) NOT NULL,
  fsd_id DECIMAL(18,0),
  flwsht_nm STRING,
  flwsht_row_num STRING,
  meas_val STRING,
  meas_typ STRING,
  rcrd_tm TIMESTAMP,
  entry_user_id STRING,
  row_insert_tsp TIMESTAMP,
  row_updt_tsp TIMESTAMP,
  insert_user_id STRING,
  updt_user_id STRING,
  msg_enqueued_tsp TIMESTAMP)
USING delta
LOCATION '{}/raw/rltm/epic/flwsht'""".format(os.getenv("adls_file_path"))

spark.sql(drop_table)
spark.sql(create_table)