# Databricks notebook source
import os

# COMMAND ----------

dbutils.fs.rm('{}/raw/rltm/epic/encntr_dx_hist'.format(os.getenv("adls_file_path")),True)

# COMMAND ----------

drop_table = "drop table if exists epic_rltm.encntr_dx_hist"

create_table = """
CREATE TABLE epic_rltm.encntr_dx_hist (
  msg_src STRING NOT NULL,
  msg_tm TIMESTAMP,
  pat_enc_csn_id DECIMAL(18,0) NOT NULL,
  dx_icd_cd STRING,
  dx_nm STRING,
  dx_cd_typ STRING,
  row_insert_tsp TIMESTAMP,
  row_updt_tsp TIMESTAMP,
  insert_user_id STRING,
  updt_user_id STRING,
  msg_enqueued_tsp TIMESTAMP)
USING delta
LOCATION '{}/raw/rltm/epic/encntr_dx_hist'""".format(os.getenv("adls_file_path"))

spark.sql(drop_table)
spark.sql(create_table)

# COMMAND ----------

