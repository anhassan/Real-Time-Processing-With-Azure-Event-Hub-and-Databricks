# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook reads messages from Service Bus Topic and write data to Synapse ADT Hist Tables
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 09/01/2022     AHASSAN           Initial creation

# COMMAND ----------

import datetime
from pyspark.sql.functions import *

def get_truncated_records(insert_table,master_dtl_table):

  adt_hist_csns_df = spark.sql("SELECT * FROM {}".format(insert_table)) \
                       .select("pat_enc_csn_id") \
                       .distinct()\
                       .withColumn("retain_record",lit(True))

  adt_dtl_df = spark.sql("SELECT * FROM {}".format(master_dtl_table))
  
  df_truncated = adt_dtl_df.join(adt_hist_csns_df,["pat_enc_csn_id"],"left")\
                           .where(col("retain_record").isNotNull())\
                           .drop("retain_record")
  return df_truncated

def truncate_child_on_expiry(df_master_truncated,table_name):
  
  df_child = spark.sql("SELECT * FROM {}".format(table_name))
  df_truncated_child = df_master_truncated.select("pat_enc_csn_id")\
                                          .join(df_child,["pat_enc_csn_id"],"inner")
  
  df_truncated_child.write.format("delta").mode("overwrite").saveAsTable(table_name)
                                          

def truncate_master_on_expiry(df_master_truncated,table_name):
    df_master_truncated.write.format("delta").mode("overwrite").saveAsTable(table_name)

def truncate_dtl_on_expiry(insert_table,dtl_table):

  adt_hist_csns_df = spark.sql("SELECT * FROM {}".format(insert_table)) \
                       .select("pat_enc_csn_id") \
                       .distinct()

  adt_dtl_df = spark.sql("SELECT * FROM {}".format(dtl_table))
  adt_dtl_trunc_df = adt_hist_csns_df.join(adt_dtl_df,["pat_enc_csn_id"],"inner")
  adt_dtl_trunc_df.write.format("delta").mode("overwrite").saveAsTable(dtl_table)
  
def optimize_table_col(table_name,z_order_col):
    spark.sql("OPTIMIZE {} ZORDER BY ({})".format(table_name,z_order_col))

def optimize_table(table_name):
  spark.sql("OPTIMIZE {}".format(table_name))

# COMMAND ----------

schema = "epic_rltm"
insert_table = "adt_hist"
master_table = "adt_dtl"
child_tables = ["encntr_dx",
                "encntr_er_complnt",
                "encntr_visit_rsn",
                "encntr_nte"]
z_order_cols = 5*["pat_enc_csn_id"]

# COMMAND ----------

for table in child_tables + [insert_table,master_table] :
  print("table name : {}".format(table))
  #spark.sql("FSCK REPAIR TABLE {}.{}".format(schema,table))

# COMMAND ----------

insert_table_name = "{}.{}".format(schema,insert_table)
master_table_name = "{}.{}".format(schema,master_table)
df_master_truncated = get_truncated_records(insert_table_name,master_table_name)

num_tables_truncated = 0

while num_tables_truncated < len(child_tables)+1:
  try:
      for child_table in child_tables:
        child_table_name = "{}.{}".format(schema,child_table)
        truncate_child_on_expiry(df_master_truncated,child_table_name)
        print("Truncated Child Table Records for Table : {} ".format(child_table))
        num_tables_truncated +=1

      print("All Child Tables have been truncated")

      truncate_master_on_expiry(df_master_truncated,master_table_name)
      print("Truncated Master Table Records for Table : {} ".format(master_table_name))
      num_tables_truncated +=1
  except Exception as error:
      print("Error : {}".format(error))
      num_tables_truncated = 0
      print("Retrying Truncation Process")

# COMMAND ----------

tables = [master_table] + child_tables
num_tables_optimized = 0

while num_tables_optimized < len(tables):
  for index,table in enumerate(tables):
    try:
        start_time = datetime.datetime.now()
        table_name = "{}.{}".format(schema,table)
        print("Starting Optimization for Table : {}".format(table_name))
        optimize_table_col(table_name,z_order_cols[index])
        end_time = datetime.datetime.now()
        print("Ended Optimization for Table : {} in {} second".format(table_name,end_time-start_time))
        num_tables_optimized += 1
    except Exception as error:
        num_tables_optimized = 0
        print("Retrying Optimization for Table : {}".format(table))
        print("Error : {}".format(error))
    