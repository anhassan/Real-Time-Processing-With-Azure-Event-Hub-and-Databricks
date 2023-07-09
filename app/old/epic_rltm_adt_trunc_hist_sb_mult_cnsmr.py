# Databricks notebook source
import datetime
from pyspark.sql.functions import *

def get_truncated_records(table_name,num_retention_days):
  
    df = spark.sql("SELECT * FROM {}".format(table_name))
    
    df_expired_csn = df.where(col("hosp_disch_time").isNotNull()) \
                       .where( to_date(col("hosp_disch_time")) < date_sub(to_date(current_timestamp()),num_retention_days) ) \
                       .withColumn("retain_record",lit(True)) \
                       .select("pat_enc_csn_id","retain_record")

    df_truncated =  df.join(df_expired_csn,["pat_enc_csn_id"],"left") \
                      .where(col("retain_record").isNull()) \
                      .drop("retain_record")
    
    return df_truncated
  
  
def truncate_child_on_expiry(df_master_truncated,table_name):
    
    df = spark.sql("SELECT * FROM {}".format(table_name))
    
    df_distinct_expired_csn =  df_master_truncated.select("pat_enc_csn_id")\
                                                  .distinct()\
                                                  .withColumn("retain_record",lit(True))
    
    df_child_truncated = df.join(df_distinct_expired_csn,["pat_enc_csn_id"],"left")\
                           .where(col("retain_record").isNotNull())\
                           .drop("retain_record")
      
    df_child_truncated.write.format("delta").mode("overwrite").saveAsTable(table_name)
    
    
def truncate_master_on_expiry(df_master_truncated,table_name):
    df_master_truncated.write.format("delta").mode("overwrite").saveAsTable(table_name)
  
     
def optimize_table(table_name,z_order_col):
    spark.sql("OPTIMIZE {} ZORDER BY ({})".format(table_name,z_order_col))

# COMMAND ----------

schema = "epic_rltm"
master_table = "adt_hist_mult_cnsmr"
child_tables = ["encntr_dx_hist_mult_cnsmr","encntr_er_complnt_hist_mult_cnsmr","encntr_visit_rsn_hist_mult_cnsmr"]
z_order_cols = 4*["row_insert_tsp"]
retention_period = 2

# COMMAND ----------

for table in child_tables + [master_table] :
  spark.sql("FSCK REPAIR TABLE {}.{}".format(schema,table))

# COMMAND ----------

master_table_name = "{}.{}".format(schema,master_table)
df_master_truncated = get_truncated_records(master_table_name,retention_period)

for child_table in child_tables:
  spark.sql("FSCK REPAIR TABLE {}.{}".format(schema,table))
  child_table_name = "{}.{}".format(schema,child_table)
  truncate_child_on_expiry(df_master_truncated,child_table_name)
  print("Truncated Child Table Records for Table : {} for past {} days".format(child_table,retention_period))
  
print("All Child Tables have been truncated for past {} days".format(retention_period))

truncate_master_on_expiry(df_master_truncated,master_table_name)
print("Truncated Master Table Records for Table : {} for past {} days".format(master_table_name,retention_period))

# COMMAND ----------

tables = [master_table] + child_tables

for index,table in enumerate(tables):
  start_time = datetime.datetime.now()
  table_name = "{}.{}".format(schema,table)
  print("Starting Optimization for Table : {}".format(table_name))
  optimize_table(table_name,z_order_cols[index])
  end_time = datetime.datetime.now()
  print("Ended Optimization for Table : {} in {} second".format(table_name,end_time-start_time))