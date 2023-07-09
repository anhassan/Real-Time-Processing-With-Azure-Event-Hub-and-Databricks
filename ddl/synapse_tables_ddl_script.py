# Databricks notebook source
# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key)
tempDir = "abfss://" + synapse_container + "@"+ storage_account_name + ".dfs.core.windows.net/temp-data"

# COMMAND ----------

def write_to_synapse(df,table_name,mode="overwrite"):
  df.write \
    .format("com.databricks.spark.sqldw") \
    .mode(mode) \
    .option("url", synapse_jdbc) \
    .option("useAzureMSI", "true") \
    .option("dbtable", table_name) \
    .option("tempdir", tempDir) \
    .save()

# COMMAND ----------

insert_tables = ["adt_hist","encntr_dx_hist","encntr_er_complnt_hist","encntr_visit_rsn_hist"]
dtl_tables = ["adt_dtl","encntr_dx","encntr_er_complnt","encntr_visit_rsn"]

tables = insert_tables + dtl_tables

for table in tables:
  df_schema = spark.sql("SELECT * FROM epic_rltm.{}".format(table)).schema
  df = spark.createDataFrame([],df_schema)
  write_to_synapse(df,"{}.{}".format("epic_rltm",table))