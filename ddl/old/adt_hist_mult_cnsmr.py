# Databricks notebook source
import os

# COMMAND ----------

dbutils.fs.rm('{}/raw/epic_rltm/adt_hist_mult_cnsmr'.format(os.getenv("adls_file_path")),True)

# COMMAND ----------

drop_table = "drop table if exists epic_rltm.adt_hist_mult_cnsmr"

create_table = """
CREATE TABLE epic_rltm.adt_hist_mult_cnsmr (
  msg_typ STRING,
  msg_src STRING NOT NULL,
  trigger_evnt STRING,
  msg_tm TIMESTAMP,
  msg_nm STRING,
  bod_id STRING,
  pat_mrn_id STRING,
  pat_enc_csn_id DECIMAL(18,0) NOT NULL,
  birth_date TIMESTAMP,
  death_date TIMESTAMP,
  pat_class_abbr STRING,
  hosp_admsn_time TIMESTAMP,
  hosp_disch_time TIMESTAMP,
  department_abbr STRING,
  loc_abbr STRING,
  oom_nm STRING,
  bed_label STRING,
  bed_status STRING,
  sex_abbr STRING,
  means_of_arrv_abbr STRING,
  acuity_level_abbr STRING,
  ed_disposition_abbr STRING,
  disch_disp_abbr STRING,
  adt_arrival_time TIMESTAMP,
  hsp_account_id DECIMAL(18,0),
  accommodation_abbr STRING,
  user_id STRING,
  row_insert_tsp TIMESTAMP,
  row_updt_tsp TIMESTAMP,
  insert_user_id STRING,
  update_user_id STRING)
USING delta
LOCATION '{}/raw/epic_rltm/adt_hist_mult_cnsmr'""".format(os.getenv("adls_file_path"))

spark.sql(drop_table)
spark.sql(create_table)

# COMMAND ----------

