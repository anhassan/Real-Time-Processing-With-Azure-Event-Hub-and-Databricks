# Databricks notebook source
# MAGIC %md
# MAGIC ###***Creating Batches of Data***

# COMMAND ----------

import dateutil.parser
from datetime import datetime
from pytz import timezone
import pandas as pd


time_string = "2020-01-01 08:{}"

msg_tm = ["00","05","06","08","10","12","14","16","18"]
admit_tm = ["01","06","07","09","11","13","14","17","17"]
hosp_admsn_tm = ["02","07","08","10","12","14","15","18","18"]
hosp_disch_tm = ["55","56","50","42","58","55","56","48",59]
row_insert_tm = 5*["20"] + 5*["25"]
csn = ["123","123","456","456","867","867","867","123","456"]
msg_type = ["A08","A01","A08","A03","A08","A04","A01","A04","A04"]
data = []
column_names = ["csn","msg_typ","msg_tm","adt_tm","hosp_admsn_tm","hosp_disch_tm","row_insert_tsp","update_insert_tsp"]

for index in range(0,len(csn)):
  msg_time = dateutil.parser.parse(time_string.format(msg_tm[index])) 
  admit_time = dateutil.parser.parse(time_string.format(admit_tm[index])) if admit_tm[index] is not None else None
  hosp_admsn_time = dateutil.parser.parse(time_string.format(hosp_admsn_tm[index])) if hosp_admsn_tm[index] is not None else None
  hosp_disch_time = dateutil.parser.parse(time_string.format(hosp_disch_tm[index])) if hosp_disch_tm[index] is not None else None
  row_insert_tsp = dateutil.parser.parse(time_string.format(row_insert_tm[index])) if row_insert_tm[index] is not None else None
  update_tm = None
  data.append([csn[index],msg_type[index],msg_time,admit_time,hosp_admsn_time,hosp_disch_time,row_insert_tsp,update_tm])

df1 = pd.DataFrame(data,columns=column_names)

  
df1

# COMMAND ----------


time_string = "2020-01-01 08:{}"

msg_tm = ["20","21","22","25","26","27"]
admit_tm = ["21","22","23","26","27","28"]
hosp_admsn_tm = ["22","23","26","27","28","30"]
hosp_disch_tm = ["35","38","39","45","44","40"]
row_insert_tm = 3*["30"] + 3*["35"]
csn = ["123","456","867","911","911","123"]
msg_type = ["A03","A13","A11","A01","A04","A08"]
data = []
column_names = ["csn","msg_typ","msg_tm","adt_tm","hosp_admsn_tm","hosp_disch_tm","row_insert_tsp","update_insert_tsp"]

for index in range(0,len(csn)):
  msg_time = dateutil.parser.parse(time_string.format(msg_tm[index])) 
  admit_time = dateutil.parser.parse(time_string.format(admit_tm[index])) if admit_tm[index] is not None else None
  hosp_admsn_time = dateutil.parser.parse(time_string.format(hosp_admsn_tm[index])) if hosp_admsn_tm[index] is not None else None
  hosp_disch_time = dateutil.parser.parse(time_string.format(hosp_disch_tm[index])) if hosp_disch_tm[index] is not None else None
  row_insert_tsp = dateutil.parser.parse(time_string.format(row_insert_tm[index])) if row_insert_tm[index] is not None else None
  update_tm = None
  data.append([csn[index],msg_type[index],msg_time,admit_time,hosp_admsn_time,hosp_disch_time,row_insert_tsp,update_tm])

df2 = pd.DataFrame(data,columns=column_names)

  
df2

# COMMAND ----------


time_string = "2020-01-01 08:{}"

msg_tm = ["30","31","32"]
admit_tm = ["31","32","36"]
hosp_admsn_tm = ["32","33","37"]
hosp_disch_tm = ["33","34","38"]
row_insert_tm = ["32","40","32"]
csn = ["123","911","123"]
msg_type = ["A04","A04","A04"]
data = []
column_names = ["csn","msg_typ","msg_tm","adt_tm","hosp_admsn_tm","hosp_disch_tm","row_insert_tsp","update_insert_tsp"]

for index in range(0,len(csn)):
  msg_time = dateutil.parser.parse(time_string.format(msg_tm[index])) 
  admit_time = dateutil.parser.parse(time_string.format(admit_tm[index])) if admit_tm[index] is not None else None
  hosp_admsn_time = dateutil.parser.parse(time_string.format(hosp_admsn_tm[index])) if hosp_admsn_tm[index] is not None else None
  hosp_disch_time = dateutil.parser.parse(time_string.format(hosp_disch_tm[index])) if hosp_disch_tm[index] is not None else None
  row_insert_tsp = dateutil.parser.parse(time_string.format(row_insert_tm[index])) if row_insert_tm[index] is not None else None
  update_tm = None
  data.append([csn[index],msg_type[index],msg_time,admit_time,hosp_admsn_time,hosp_disch_time,row_insert_tsp,update_tm])

df3 = pd.DataFrame(data,columns=column_names)

  
df3

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType

struct_fields = [StructField("csn",StringType(),True),
                StructField("msg_typ",StringType(),True),
                StructField("msg_tm",TimestampType(),True),
                StructField("adt_tm",TimestampType(),True),
                StructField("hosp_admsn_tm",TimestampType(),True),
                StructField("hosp_disch_tm",TimestampType(),True),
                StructField("row_insert_tsp",TimestampType(),True),
                StructField("update_insert_tsp",TimestampType(),True)]

schema = StructType(struct_fields)

# COMMAND ----------

batch1 = spark.createDataFrame(df1,schema)
batch2 = spark.createDataFrame(df2,schema)
batch3 = spark.createDataFrame(df3,schema)

# COMMAND ----------

display(batch1)

# COMMAND ----------

display(batch2)

# COMMAND ----------

display(batch3)

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Create Detail Table***

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dtl_table(
# MAGIC csn String,
# MAGIC msg_typ String,
# MAGIC msg_tm Timestamp,
# MAGIC adt_tm Timestamp,
# MAGIC hosp_admsn_tm Timestamp,
# MAGIC hosp_disch_tm Timestamp,
# MAGIC row_insert_tsp Timestamp,
# MAGIC update_insert_tsp Timestamp
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE dtl_table;

# COMMAND ----------

from delta.tables import *
dtl_df = DeltaTable.forPath(spark, 'dbfs:/user/hive/warehouse/dtl_table')

# COMMAND ----------

# MAGIC %md
# MAGIC ###***Change Data Capture (CDC) and Deduplication***

# COMMAND ----------

def add_adt_arrival_tm_batch(batch,batch_reduced):
  return add_col_batch(batch,batch_reduced,["A04"],"adt_tm").drop(*["hosp_admsn_tm","hosp_disch_tm"])

# COMMAND ----------

def add_hosp_admsn_tm_batch(batch,batch_reduced):
  return add_col_batch(batch,batch_reduced,["A01","A11"],"hosp_admsn_tm").select(*["csn","hosp_admsn_tm","modifier_hosp_admsn_tm"])

# COMMAND ----------

def add_hosp_disch_tm_batch(batch,batch_reduced):
  return add_col_batch(batch,batch_reduced,["A03","A13"],"hosp_disch_tm").select(*["csn","hosp_disch_tm","modifier_hosp_disch_tm"])

# COMMAND ----------

def add_all_cols_batch(batch,batch_reduced):
  return   add_adt_arrival_tm_batch(batch,batch_reduced) \
           .join(add_hosp_admsn_tm_batch(batch,batch_reduced),["csn"])\
           .join(add_hosp_disch_tm_batch(batch,batch_reduced),["csn"])

# COMMAND ----------

from pyspark.sql.functions import *

def add_col_batch(batch,batch_reduced,trig_tags,col_name):
  
  window = Window.partitionBy("csn")
  
  if len(trig_tags) == 1:
    batch_filtered = batch.where(col("msg_typ")==trig_tags[0]) \
                          .withColumn("max_time",max(col("msg_tm")).over(window)) \
                          .where(col("msg_tm")== col("max_time")) \
                          .drop(col("max_time")) \
                          .withColumn("modifier_"+col_name,lit("modified")) \
                          .select(*["csn","modifier_"+col_name,col_name])
  if len(trig_tags) > 1:
    batch_filtered =   batch.where((col("msg_typ")==trig_tags[0]) | (col("msg_typ")==trig_tags[1])) \
                            .withColumn("max_time",max(col("msg_tm")).over(window)) \
                            .where(col("msg_tm") == col("max_time")) \
                            .drop(col("max_time")) \
                            .withColumn(col_name+"_tmp",when(col("msg_typ")==trig_tags[1],None).otherwise(batch[col_name])) \
                            .drop(col(col_name)) \
                            .withColumnRenamed(col_name+"_tmp",col_name)\
                            .withColumn("modifier_"+col_name,lit("modified"))\
                            .select(*["csn","modifier_"+col_name,col_name])
  
  batch_col_added    = batch_reduced.drop(col(col_name)) \
                                    .join(batch_filtered,["csn"],"left") \
                                    .na.fill("unmodified",["modifier_"+col_name])
  
  return batch_col_added

# COMMAND ----------

from pyspark import Row
from pyspark.sql.functions import col,max
from pyspark.sql import Window
from pyspark.sql import functions as F

def get_transformed_batch(batch):
  
  window = Window.partitionBy("csn")
  batch_reduced = batch.withColumn("max_time",max(col("msg_tm")).over(window)) \
                  .where(col("msg_tm") == col("max_time")) \
                  .drop(col("max_time"))
  batch_transformed_tm = add_all_cols_batch(batch,batch_reduced)
  return batch_transformed_tm

# COMMAND ----------

batch1_transformed = get_transformed_batch(batch1)
batch2_transformed = get_transformed_batch(batch2)
batch3_transformed = get_transformed_batch(batch3)

# COMMAND ----------

display(batch1_transformed)

# COMMAND ----------

display(batch2_transformed)

# COMMAND ----------

display(batch3_transformed)

# COMMAND ----------

def get_update_condition(df,table_name,exclude_cols):
  columns = df.toDF().columns
  cond = {}
  for col in columns:
    if col not in exclude_cols:
      cond[col] = "{}.{}".format(table_name,col)
  cond["update_insert_tsp"] = "{}.row_insert_tsp".format(table_name)
  return cond

# COMMAND ----------

def get_insert_condition(df,table_name):
  columns = df.toDF().columns
  cond = {}
  for col in columns:
    cond[col] = "{}.{}".format(table_name,col)
  return cond

# COMMAND ----------

from delta.tables import *
dtl_df = DeltaTable.forPath(spark, 'dbfs:/user/hive/warehouse/dtl_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dtl_table;

# COMMAND ----------

def change_data_capture_dedup(dtl_df,batch_transformed):
  
  dtl_df.alias("dtl") \
                 .merge(
                  batch_transformed.alias("hist"),
                  "dtl.csn = hist.csn") \
                  .whenMatchedUpdate(""" hist.modifier_adt_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "unmodified" """, \
                                     set=get_update_condition(dtl_df,"hist",["update_insert_tsp","row_insert_tsp","adt_tm","hosp_admsn_tm","hosp_disch_tm"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "modified" """, \
                                     set=get_update_condition(dtl_df,"hist",["update_insert_tsp","row_insert_tsp","adt_tm","hosp_admsn_tm"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "unmodified" """, 
                                     set=get_update_condition(dtl_df,"hist",["update_insert_tsp","row_insert_tsp","adt_tm","hosp_disch_tm"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_tm = "modified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "unmodified" """, 
                                     set=get_update_condition(dtl_df,"hist",["update_insert_tsp","row_insert_tsp","hosp_admsn_tm","hosp_disch_tm"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "modified" """, 
                                     set=get_update_condition(dtl_df,"hist",["update_insert_tsp","row_insert_tsp","adt_tm"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_tm = "modified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "modified" """, 
                                     set=get_update_condition(dtl_df,"hist",["update_insert_tsp","row_insert_tsp","hosp_admsn_tm"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_tm = "modified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "unmodified" """, 
                                     set=get_update_condition(dtl_df,"hist",["update_insert_tsp","row_insert_tsp","hosp_disch_tm"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_tm = "modified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "modified" """, 
                                     set=get_update_condition(dtl_df,"hist",["update_insert_tsp","row_insert_tsp"])) \
                  \
                  .whenNotMatchedInsert(values=get_insert_condition(dtl_df,"hist")) \
                  \
                  .execute()

# COMMAND ----------

change_data_capture_dedup(dtl_df,batch1_transformed)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dtl_table;

# COMMAND ----------

change_data_capture_dedup(dtl_df,batch2_transformed)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dtl_table;

# COMMAND ----------

change_data_capture_dedup(dtl_df,batch3_transformed)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dtl_table;