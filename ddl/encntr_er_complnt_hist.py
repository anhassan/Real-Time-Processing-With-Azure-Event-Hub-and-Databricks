# Databricks notebook source
import os

# COMMAND ----------

dbutils.fs.rm('{}/raw/rltm/epic/encntr_er_complnt_hist'.format(os.getenv("adls_file_path")),True)

# COMMAND ----------

drop_table = "drop table if exists epic_rltm.encntr_er_complnt_hist"

create_table = """
CREATE TABLE epic_rltm.encntr_er_complnt_hist (
  msg_src STRING NOT NULL,
  msg_tm TIMESTAMP,
  pat_enc_csn_id DECIMAL(18,0) NOT NULL,
  er_complnt STRING,
  row_insert_tsp TIMESTAMP,
  row_updt_tsp TIMESTAMP,
  insert_user_id STRING,
  updt_user_id STRING,
  msg_enqueued_tsp TIMESTAMP)
USING delta
LOCATION '{}/raw/rltm/epic/encntr_er_complnt_hist'""".format(os.getenv("adls_file_path"))

spark.sql(drop_table)
spark.sql(create_table)

# COMMAND ----------

