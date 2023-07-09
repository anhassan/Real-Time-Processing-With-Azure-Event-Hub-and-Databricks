# Databricks notebook source
import os

# COMMAND ----------

dbutils.fs.rm('{}/raw/rltm/epic/adt_hist'.format(os.getenv("adls_file_path")),True)

# COMMAND ----------

drop_table = "drop table if exists epic_rltm.adt_hist"

create_table = """
CREATE TABLE epic_rltm.adt_hist (
  msg_typ STRING,
  msg_src STRING NOT NULL,
  trigger_evnt STRING,
  msg_tm TIMESTAMP,
  msg_nm STRING,
  bod_id STRING,
  pat_mrn_id STRING,
  pat_enc_csn_id DECIMAL(18,0) NOT NULL,
  birth_dt TIMESTAMP,
  death_dt TIMESTAMP,
  pat_class_abbr STRING,
  hosp_admsn_tm TIMESTAMP,
  hosp_disch_tm TIMESTAMP,
  dept_abbr STRING,
  loc_abbr STRING,
  room_nm STRING,
  bed_label STRING,
  bed_stat STRING,
  sex_abbr STRING,
  means_of_arrv_abbr STRING,
  acuity_level_abbr STRING,
  ed_dsptn_abbr STRING,
  disch_dsptn_abbr STRING,
  adt_arvl_tm TIMESTAMP,
  hsp_acct_id DECIMAL(18,0),
  accommodation_abbr STRING,
  user_id STRING,
  row_insert_tsp TIMESTAMP,
  row_updt_tsp TIMESTAMP,
  insert_user_id STRING,
  updt_user_id STRING,
  msg_enqueued_tsp TIMESTAMP,
  pregnancy_flg STRING)
USING delta
LOCATION '{}/raw/rltm/epic/adt_hist'""".format(os.getenv("adls_file_path"))

spark.sql(drop_table)
spark.sql(create_table)