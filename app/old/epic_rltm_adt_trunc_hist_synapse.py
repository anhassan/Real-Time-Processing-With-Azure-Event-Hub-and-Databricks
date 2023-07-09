# Databricks notebook source
# TYPE: PYTHON commands
# DEFINITION: This notebook deletes encounters discharged 2 days prior to today from ADT Hist Tables in Synapse
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 08-19-2022     AHASSAN2          INITIAL CREATION

# COMMAND ----------

from pyspark.sql.types import StringType,IntegerType
from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC %run ../../../etl/data_engineering/commons/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create dataframe that captures all encounters discharged prior to 2 days from today

# COMMAND ----------


df = spark.read\
  .format("com.databricks.spark.sqldw") \
  .option("url", synapse_jdbc) \
  .option("tempDir", tempDir) \
  .option("useAzureMSI", "true") \
  .option("query",f'select * from epic_rltm.adt_hist_mult_cnsmr where hosp_disch_time is not null')\
  .load()\
  .filter('hosp_disch_time < date_add(current_timestamp,-2)')
  


# COMMAND ----------

# MAGIC %md
# MAGIC ####Gather data for Visit Reason and Enctr diagnosis tables for discharged encounters

# COMMAND ----------

encntr_visit_rsn_df = spark.read\
                           .format("com.databricks.spark.sqldw") \
                           .option("url", synapse_jdbc) \
                           .option("tempDir", tempDir) \
                           .option("useAzureMSI", "true") \
                           .option("dbtable","epic_rltm.encntr_visit_rsn_hist_mult_cnsmr")\
                           .load()


encntr_visit_rsn_df_del =  encntr_visit_rsn_df.join(df,["pat_enc_csn_id"],"inner")\
                          .select(encntr_visit_rsn_df.msg_src,encntr_visit_rsn_df.msg_tm,encntr_visit_rsn_df.pat_enc_csn_id,encntr_visit_rsn_df.encntr_rsn,encntr_visit_rsn_df.row_insert_tsp,encntr_visit_rsn_df.insert_user_id)
                             

# COMMAND ----------

encntr_dx_df = spark.read\
                           .format("com.databricks.spark.sqldw") \
                           .option("url", synapse_jdbc) \
                           .option("tempDir", tempDir) \
                           .option("useAzureMSI", "true") \
                           .option("dbtable","epic_rltm.encntr_dx_hist_mult_cnsmr")\
                           .load()

encntr_dx_df_del =  encntr_dx_df.join(df,["pat_enc_csn_id"],"inner")\
                          .select(encntr_dx_df.msg_src,encntr_dx_df.msg_tm,encntr_dx_df.pat_enc_csn_id,encntr_dx_df.dx_icd_cd,encntr_dx_df.dx_name,encntr_dx_df.dx_code_type,encntr_dx_df.row_insert_tsp,encntr_dx_df.insert_user_id)
                             

# COMMAND ----------

encs_to_be_deleted  = df.withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast(IntegerType())) \
                        .select("pat_enc_csn_id").distinct().rdd.flatMap(lambda x: x).collect()


# COMMAND ----------

write_to_synapse(df,'epic_rltm.adt_hist_mult_cnsmr',mode='append',delete_query=f'delete from epic_rltm.adt_hist_mult_cnsmr where pat_enc_csn_id in ' + format(tuple(encs_to_be_deleted)))
write_to_synapse(encntr_visit_rsn_df_del,'epic_rltm.encntr_visit_rsn_hist_mult_cnsmr',mode='append',delete_query=f'delete from epic_rltm.encntr_visit_rsn_hist_mult_cnsmr where pat_enc_csn_id in ' + format(tuple(encs_to_be_deleted)))
write_to_synapse(encntr_dx_df_del,'epic_rltm.encntr_dx_hist_mult_cnsmr',mode='append',delete_query=f'delete from epic_rltm.encntr_dx_hist_mult_cnsmr where pat_enc_csn_id in ' + format(tuple(encs_to_be_deleted)))