# Databricks notebook source
# MAGIC %run ../../../etl/data_engineering/commons/utilities

# COMMAND ----------

def read_from_synapse(table_name):
  return spark.read \
              .format("com.databricks.spark.sqldw") \
              .option("url", synapse_jdbc) \
              .option("tempDir", tempDir) \
              .option("useAzureMSI", "true") \
              .option("dbTable", table_name) \
              .load()

# COMMAND ----------

from pyspark.sql.functions import lit

def trunc_synapse_table(table_name):
  df = read_from_synapse(table_name)
  df_empty = spark.createDataFrame([],df.schema)
  write_to_synapse(df_empty,table_name)

def add_col_synapse(table_name,col_name,col_type):
  df = read_from_syanpse(table_name)
  df = df.withColumn(col_name,lit(None).cast(col_type))
  write_to_synapse(df,table_name)

def trunc_delta_table(table_name):
  spark.sql("DELETE FROM {}".format(table_name))