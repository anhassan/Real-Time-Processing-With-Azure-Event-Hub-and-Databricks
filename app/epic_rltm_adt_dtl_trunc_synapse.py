# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook reads messages from Service Bus Topic and write data to Synapse ADT Hist Tables
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 09/01/2022     AHASSAN           Initial creation

# COMMAND ----------

# MAGIC %run ../utils/utilities

# COMMAND ----------

import numpy as np
import math 
from decimal import *

def split_list(input_list,max_size=30000):
  num_splits = int(math.ceil(len(input_list)/max_size))
  splitted_arr = np.array_split(np.array(input_list),num_splits)
  splitted_list = [list(split) for split in splitted_arr if split.size > 0]
  return splitted_list

def render_data(table_schema):
  data_dict = {"msg_src":"STL","pat_enc_csn_id" : Decimal(-1), "updt_user_id" : "DEL_USER"}
  return [[data_dict[column] if column in data_dict.keys() else None for column in table_schema.names]]


# COMMAND ----------

from decimal import *

def read_from_synapse(table_name):
  return spark.read\
       .format("com.databricks.spark.sqldw")\
       .option("url",synapse_jdbc)\
       .option("tempDir",tempDir)\
       .option("useAzureMSI","true")\
       .option("query","SELECT * FROM {}".format(table_name))\
       .load()

def delete_from_synapse(table_name,table_schema,write_vals,delete_col,delete_keys):
  split_delete_keys = split_list(delete_keys)
  for del_keys_list in split_delete_keys:
    write_df = spark.createDataFrame(write_vals,table_schema)
    delete_query = "DELETE FROM {} WHERE {} IN ({})".format(table_name,delete_col,",".join(del_keys_list+["-1"]))

    write_df.write\
            .format("com.databricks.spark.sqldw")\
            .mode("append")\
            .option("useAzureMSI","true")\
            .option("url",synapse_jdbc)\
            .option("dbtable",table_name)\
            .option("tempdir",tempDir)\
            .option("postActions",delete_query)\
            .save()
  

# COMMAND ----------

def get_truncate_records(schema_name,hist_table,dtl_table):
  hist_df = read_from_synapse("{}.{}".format(schema_name,hist_table))
  dtl_df = read_from_synapse("{}.{}".format(schema_name,dtl_table))
  
  hist_csns = hist_df.select("pat_enc_csn_id").distinct()
  del_csns_df = dtl_df.join(hist_csns,["pat_enc_csn_id"],"leftanti")\
                      .select("pat_enc_csn_id")\
                      .distinct()\
                      .collect()
  del_csns = [str(row["pat_enc_csn_id"]) for row in del_csns_df]
  return del_csns


def delete_truncate_records(schema_name,table_name,delete_col,delete_keys):
  table_schema = read_from_synapse("{}.{}".format(schema_name,table_name)).schema
  delete_from_synapse("{}.{}".format(schema_name,table_name),table_schema,render_data(table_schema),delete_col,delete_keys)


# COMMAND ----------

write_vals = {
  "adt_dtl" : [[None,"STL"] + 5*[None] + [Decimal(-1)] + 23*[None]],
  "encntr_dx" : [[Decimal(-1)] + 9*[None]],
  "encntr_er_complnt" : [["STL",None,Decimal(-1)] + 5*[None]],
  "encntr_visit_rsn" : [[Decimal(-1),"STL",None] + 5*[None]]
}

# COMMAND ----------

schema = "epic_rltm"
hist_table = "adt_hist"
master_table = "adt_dtl"
child_tables = ["encntr_dx","encntr_er_complnt","encntr_visit_rsn", "encntr_nte"]
delete_col = "pat_enc_csn_id"

# COMMAND ----------

truncate_records = get_truncate_records(schema,hist_table,master_table)

for child_table in child_tables:
  delete_truncate_records(schema,child_table,delete_col,truncate_records)
  print("Truncated Child Table Records for Table : {} ".format(child_table))
  
print("All Child Tables have been truncated for today")

delete_truncate_records(schema,master_table,delete_col,truncate_records)
print("Truncated Master Table Records for Table : {} ".format(master_table))