# Databricks notebook source
# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key)
tempDir = "abfss://" + synapse_container + "@"+ storage_account_name + ".dfs.core.windows.net/temp-data"

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", True)
spark.conf.set("spark.sql.files.ignoreMissingFiles",True)

# COMMAND ----------

delimiter = "*&*&*&*&*&*&*&*&*&*&*&*&*&*&*&*"

# COMMAND ----------

def get_fully_qualified_tag(message,partial_tag):
  possible_tags = [col for col in message.columns if col.endswith(partial_tag)]
  return possible_tags[0]

def get_fully_qualified_obj_tag(message,partial_tag):
  possible_tags = [col for col in message.columns if partial_tag in col]
  obj_tag = possible_tags[0][0:possible_tags[0].find(partial_tag)] + partial_tag
  return obj_tag

# COMMAND ----------

def read_xml_batchs(batch_msgs):
  try:
    msgs = batch_msgs.split(delimiter)
    mult_msgs = [ msg[1:] if msg[0] == "\n" else msg for msg in msgs ]
  except:
    mult_msgs = [ batch_msgs[1:] if batch_msgs[0] == "\n" else batch_msgs ]
  return mult_msgs

# COMMAND ----------

from opencensus.ext.azure.log_exporter import AzureLogHandler
import logging

def init_logs():
  azure_logger = logging.getLogger('azure_streamingapp_logger')
  azure_logger.setLevel(logging.DEBUG)
  #connection_string = 'InstrumentationKey=1af6167a-b3e4-445c-94e6-667d3118874a;IngestionEndpoint=https://northcentralus-0.in.applicationinsights.azure.com/'
  #connection_string = 'InstrumentationKey=5405a1f9-354f-4358-9aa3-cb918b45882a;IngestionEndpoint=https://northcentralus-0.in.applicationinsights.azure.com/'
  connection_string = application_insights_secret
  azure_logger.addHandler(AzureLogHandler(
      connection_string=connection_string)
  )
  return azure_logger

# COMMAND ----------

app_logger = init_logs()

# COMMAND ----------

def flatten_xml(doc,prefix=None):
  flattened = {}
  for key,value in doc.items():
    key = prefix + "." + key if prefix else key
    if isinstance(value,dict):
      flattened = {**flattened,**flatten_xml(value,key)}
    elif isinstance(value,list):
      for element in value:
        if isinstance(element,dict):
          flattened = {**flattened,**flatten_xml(element,key)}
        else:
          flattened[key] = element
    else:
      flattened[key] = value
  return flattened

def move_files(num_files,input_blobs,in_folder,out_folder):
  for index,blob in enumerate(input_blobs):
    if index > num_files:
      break
    else:
      dbutils.fs.mv(blob.path,blob.path.replace(in_folder,out_folder))


# COMMAND ----------

from pyspark.sql.functions import col, udf,lit
from pyspark.sql.types import StringType
import pandas_read_xml as pdx
import codecs
import numpy as np

def to_str(bytes):
  return codecs.decode(bytes,'UTF-8')

convertUdf = udf(lambda x: to_str(x))

# COMMAND ----------

client_id = dbutils.secrets.get(scope=secret_scope,key="IDPAutoLoaderClientID")
client_secret = dbutils.secrets.get(scope=secret_scope,key="IDPAutoLoaderClientSecret")
tenant_id = dbutils.secrets.get(scope=secret_scope,key="tenant-id")
subscription_id = dbutils.secrets.get(scope=secret_scope,key="subscription-id")
resource_group = dbutils.secrets.get(scope=secret_scope,key="resource-group")

# COMMAND ----------

cloudfile_configs = {
  "cloudFiles.format" : "binaryFile",
  "cloudFiles.useNotifications" : True,
  "cloudFiles.subscriptionId" :subscription_id,
  "cloudFiles.tenantId" : tenant_id,
  "cloudFiles.resourceGroup" : resource_group,
  "cloudFiles.clientId" : client_id,
  "cloudFiles.clientSecret" : client_secret 
}

# COMMAND ----------

cloudfile_configs_revised = {
  "cloudFiles.format" : "binaryFile",
  "cloudFiles.useIncrementalListing" :  False, # added due to the problem caused
}

# COMMAND ----------

from pyspark.sql.functions import input_file_name,split

def generic_read(directory_path):
  df = spark.readStream.format("cloudFiles") \
            .options(**cloudfile_configs_revised) \
            .option("cloudFiles.format", "binaryFile") \
            .load(directory_path) 
  return df

def read_from_directory(directory_path):
  df = generic_read(directory_path) \
            .select(convertUdf(col('content')).alias("text"))
  return df

def read_from_directory_with_filename(directory_path):
  df = generic_read(directory_path) \
            .withColumn("msg_path",input_file_name()) \
            .withColumn("msg_nm", split(col("msg_path"),'/').getItem(5)) \
            .select(*[convertUdf(col('content')).alias("text"),"msg_nm"])
  return df

# COMMAND ----------

from hl7apy import parser
from hl7apy.core import Group, Segment
from hl7apy.exceptions import UnsupportedVersion

def parse_hl7_message(message):
  try:
    msg = parser.parse_message(message.replace('\n','\r'), find_groups = False, validation_level=2)
  except:
    return None
#   except:
#     raise Exception("Invalid message")   
  return str(msg)

parseHl7Udf = udf(lambda x:parse_hl7_message(x))

# COMMAND ----------

import asyncio
from azure.eventhub.aio import EventHubConsumerClient
from datetime import datetime

def read_from_event_hub(event_hub_connection_string,event_hub_name,consumer_group):
  
  event_hub_config = {}
  event_hub_config['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(event_hub_connection_string) 
  event_hub_config['eventshubs.eventHubName'] = event_hub_name
  event_hub_config['eventhubs.consumerGroup'] = consumer_group
  event_hub_config['eventhubs.maxEventsPerTrigger'] = 1000
  #event_hub_config['eventhubs.useExclusiveReceiver'] = False
  
  df = spark.readStream \
            .format("eventhubs") \
            .options(**event_hub_config) \
            .load() \
            .withColumn("text",col("body").cast(StringType())) \
            .withColumn("msg_nm",concat_ws("-",col("partition"),col("sequenceNumber"),col("offset"))) \
            .withColumn("read_tmsp",lit(datetime.now())) \
            .select(*["text","msg_nm","read_tmsp"])
  return df

# COMMAND ----------

def process_hl7_df(df):
  cols = df.columns
  df = df.na.drop(how="all", subset=cols)
  
  drop_cols = ["row_updt_tsp","update_user_id"]
  df = df.drop(*drop_cols)
  c_user = get_current_user()
  df = df.withColumn('insert_user_id',lit(c_user))
  
  return df

# COMMAND ----------


# # Testing
# import asyncio
# from azure.eventhub.aio import EventHubConsumerClient
# from datetime import datetime

# def read_from_event_hub1(event_hub_connection_string,event_hub_name,consumer_group):
  
#   event_hub_config = {}
#   event_hub_config['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(event_hub_connection_string) 
#   event_hub_config['eventshubs.eventHubName'] = event_hub_name
#   event_hub_config['eventhubs.consumerGroup'] = consumer_group
#   event_hub_config['eventhubs.maxEventsPerTrigger'] = 1000
  
#   df = spark.readStream \
#             .format("eventhubs") \
#             .options(**event_hub_config) \
#             .load() \
#             .withColumn("text",col("body").cast(StringType())) \
#             .select(*["text"])
#   return df

# COMMAND ----------

from pyspark import Row
from pyspark.sql.functions import col,max
from pyspark.sql import Window
from pyspark.sql import functions as F


def writeToSQLWarehouse(df, epochId,output_table_name):
  df.write \
    .format("com.databricks.spark.sqldw") \
    .mode('append') \
    .option("url", synapse_jdbc) \
    .option("useAzureMSI", "true") \
    .option("dbtable", output_table_name) \
    .option("tempdir", tempDir) \
    .save()
  
# Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDelta(microBatchOutputDF,batchId,aggregated_table_name): 
  # within batch sorting

  window = Window.partitionBy("pat_enc_csn_id")
  reduced_batch = microBatchOutputDF.withColumn("max_time",max(col("msg_tm")).over(window)) \
              .where(col("msg_tm") == col("max_time"))
  reduced_batch = reduced_batch.orderBy(col("msg_tm").asc()).withColumn("row_insert_tsp",F.current_timestamp())
  reduced_batch.createOrReplaceTempView("updates")
  


  #between batch sorting
  
  reduced_batch._jdf.sparkSession().sql("""
    MERGE INTO {} t
    USING updates s
    ON s.pat_enc_csn_id = t.pat_enc_csn_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    
  """.format(aggregated_table_name))

def upsertToSynapse(microBatchOutputDF,batchId,output_table_name):
  window = Window.partitionBy("pat_enc_csn_id")
  reduced_batch = microBatchOutputDF.withColumn("max_time",max(col("msg_time")).over(window)) \
              .where(col("msg_time") == col("max_time"))
  writeToSQLWarehouse(reduced_batch, batchId,output_table_name)
  

# COMMAND ----------

def upsert_stream_to_delta(df,output_table_name,checkpoint_loc,schema):
  try:
      prev_df = spark.sql("SELECT * FROM {}".format(output_table_name))
      exists = prev_df.count()
  except:
      exists = 0
  if exists < 1:
    spark.createDataFrame([],schema).write \
    .format("delta").mode("overwrite").saveAsTable(output_table_name)
  
  df.writeStream \
  .format("delta") \
  .trigger(processingTime='1 second') \
  .option("checkpointLocation",checkpoint_loc) \
  .foreachBatch(lambda df,batchId : upsertToDelta(df,batchId,output_table_name) ) \
  .start()
  
def upsert_stream_to_synapse(df,output_table_name,checkpoint_loc):
  df.writeStream \
    .trigger(processingTime='1 second') \
    .option("checkpointLocation",checkpoint_loc) \
    .foreachBatch(lambda df,batchId : upsertToSynapse(df,batchId,output_table_name)) \
    .start()

# COMMAND ----------

def write_stream_to_delta(df,output_table_name,checkpoint_loc):
   df.writeStream \
  .format("delta") \
  .trigger(processingTime='1 second') \
  .option("checkpointLocation",checkpoint_loc) \
  .toTable(output_table_name)
    
def write_stream_to_synapse(df,output_table_name,checkpoint_loc):
   df.writeStream \
    .trigger(processingTime='1 second') \
    .option("checkpointLocation",checkpoint_loc) \
    .foreachBatch(lambda df,batchId : writeToSQLWarehouse(df,batchId,output_table_name)) \
    .start()


# COMMAND ----------

def insert_update_stream_to_delta(df,insert_table,update_table,checkpoint_loc,schema_hist,schema):
  try:
      prev_insert_df = spark.sql("SELECT * FROM {}".format(insert_table))
      insert_exists = prev_insert_df.count()
  except:
      insert_exists = 0
  try:
      prev_update_df = spark.sql("SELECT * FROM {}".format(update_table))
      update_exists = prev_update_df.count()
  except:
      update_exists = 0
      
  if insert_exists < 1:
      spark.createDataFrame([],schema).write \
      .format("delta").mode("overwrite").saveAsTable(insert_table)
      
  if update_exists < 1:
      spark.createDataFrame([],schema_hist).write \
      .format("delta").mode("overwrite").saveAsTable(update_table)
  
  df.writeStream \
  .format("delta") \
  .option("checkpointLocation",checkpoint_loc) \
  .foreachBatch(lambda df,batchId : insertUpdateToDelta(df,batchId,insert_table,update_table) ) \
  .start()

# COMMAND ----------

# Function to insert and upsert `microBatchOutputDF` into Delta table 
def insertUpdateToDelta(batch,batchId,insert_table,update_table): 
  #--------UPDATE PART-------------------------------------------
  # within batch sorting

  window = Window.partitionBy("pat_enc_csn_id","msg_src")
  reduced_batch = batch.withColumn("max_time",max(col("msg_tm")).over(window)) \
              .where(col("msg_tm") == col("max_time"))
  reduced_batch.createOrReplaceTempView("updates")
  
  #between batch sorting
  
  reduced_batch._jdf.sparkSession().sql("""
    MERGE INTO {} t
    USING updates s
    ON s.pat_enc_csn_id = t.pat_enc_csn_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    
  """.format(update_table))
  
  #--------INSERT PART----------------------------------------------------
  drop_cols = ["row_updt_tsp","update_user_id"]
  batch = batch.drop(*drop_cols)
  batch.write.format("delta").mode("append").toTable(insert_table)
  
  

# COMMAND ----------

import datetime
from datetime import datetime, timezone
import pytz

def writeWithLogs_evh(batch,batch_id,table_name):
  
  log_template = "Table Name : {} - Batch Id : {} - Batch Count : {} - Batch Latest Msg Time {} -  Batch Processing Time : {} - Current Timestamp : {}"
  start_time = datetime.now().astimezone(pytz.timezone('US/Central'))
  batch_row_count = batch.count()
  #batch_latest_msg_tm = batch.orderBy(col("msg_tm").desc()).first()["msg_tm"] if batch_row_count > 0 else ""
  batch.write.format("delta").mode("append").saveAsTable(table_name)
  end_time = datetime.now().astimezone(pytz.timezone('US/Central'))
  app_logger.info(log_template.format(table_name,batch_id,batch_row_count," ",str(end_time - start_time),end_time))

# COMMAND ----------

import datetime
from datetime import datetime, timezone
import pytz

def writeWithLogs(batch,batch_id,table_name,app_logger):
  
  log_template = "Table Name : {} - Batch Id : {} - Batch Count : {} - Batch Latest Msg Time {} -  Batch Processing Time : {} - Current Timestamp : {}"
  start_time = datetime.now().astimezone(pytz.timezone('US/Central'))
  batch_row_count = batch.count()
  batch_latest_msg_tm = batch.orderBy(col("msg_tm").desc()).first()["msg_tm"] if batch_row_count > 0 else ""
  batch.write.format("delta").mode("append").saveAsTable(table_name)
  end_time = datetime.now().astimezone(pytz.timezone('US/Central'))
  app_logger.info(log_template.format(table_name,batch_id,batch_row_count,batch_latest_msg_tm,str(end_time - start_time),end_time))
  
  

# COMMAND ----------

import datetime
from datetime import datetime, timezone
import pytz

def writeWithLogs_msg_nm(batch,batch_id,table_name,app_logger):
  
  log_template = "Table Name : {} - Batch Id : {} - Batch Count : {} - Batch Latest Msg Time {} -  Batch Processing Time : {} - logs Processing Time : {} - Current Timestamp : {}  - Batch msg nm {} - {}"
  lgstart_time = datetime.now().astimezone(pytz.timezone('US/Central'))
  batch_row_count = batch.count()
  orbatch = batch.orderBy(col("msg_tm").desc())
  batch_latest_msg_tm = orbatch.first()["msg_tm"] if batch_row_count > 0 else ""
  batch_end_msg_nm = orbatch.first()["row_insert_tsp"] if batch_row_count > 0 else ""
  batch_start_msg_nm = batch.orderBy(col("msg_tm").asc()).first()["row_insert_tsp"] if batch_row_count > 0 else ""
  start_time = datetime.now().astimezone(pytz.timezone('US/Central'))
  batch.write.format("delta").mode("append").saveAsTable(table_name)
  end_time = datetime.now().astimezone(pytz.timezone('US/Central'))
  app_logger.info(log_template.format(table_name,batch_id,batch_row_count,batch_latest_msg_tm,str(end_time - start_time),str(start_time - lgstart_time),end_time,batch_start_msg_nm,batch_end_msg_nm))
  
  

# COMMAND ----------

def get_current_user():
  query = 'select current_user' 
  current_user = spark.sql(query)
  return current_user.collect()[0][0] 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
def write_to_delta_sorted_with_logs(df,table_name,checkpoint_loc):
  drop_cols = ["row_updt_tsp","update_user_id"]
  df = df.drop(*drop_cols)
  df = df.withColumn("row_insert_tsp",current_timestamp())

  df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation",checkpoint_loc) \
  .option("mergeSchema", "true") \
  .foreachBatch(lambda df,batch_id : writeWithLogs(df,batch_id,table_name,app_logger)) \
  .start()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
def write_to_delta_sorted_with_logs_evh_atd(df,table_name,checkpoint_loc):
  drop_cols = ["row_updt_tsp","update_user_id"]
  df = df.drop(*drop_cols)
  c_user = get_current_user()
  df = df.withColumn('insert_user_id',lit(c_user))

  df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation",checkpoint_loc) \
  .option("mergeSchema", "true") \
  .foreachBatch(lambda df,batch_id : writeWithLogs_msg_nm(df,batch_id,table_name,app_logger)) \
  .start()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from pyspark import StorageLevel
def write_to_delta_sorted_with_logs_evh(df,table_name,checkpoint_loc):
  drop_cols = ["row_updt_tsp","update_user_id"]
  df = df.drop(*drop_cols)
  c_user = get_current_user()
  df = df.withColumn('insert_user_id',lit(c_user))

  df.writeStream \
  .format("delta") \
  .trigger(processingTime='1 second') \
  .outputMode("append") \
  .option("checkpointLocation",checkpoint_loc) \
  .option("mergeSchema", "true") \
  .foreachBatch(lambda df,batch_id : writeWithLogs_evh(df,batch_id,table_name,app_logger)) \
  .start()

# COMMAND ----------

def write_to_delta_sorted_with_logs_queryawait(df,table_name,checkpoint_loc):
  drop_cols = ["row_updt_tsp","update_user_id"]
  df = df.drop(*drop_cols)
  c_user = get_current_user()
  df = df.withColumn('insert_user_id',lit(c_user))

  df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation",checkpoint_loc) \
  .option("mergeSchema", "true") \
  .foreachBatch(lambda df,batch_id : writeWithLogs_evh(df,batch_id,table_name,app_logger)) \
  .start().awaitTermination()

# COMMAND ----------

def write_to_delta_sorted(df,table_name,checkpoint_loc):
  drop_cols = ["row_updt_tsp","update_user_id"]
  df = df.drop(*drop_cols)
  df = df.withColumn("row_insert_tsp",current_timestamp())
  
  df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation",checkpoint_loc) \
  .toTable(table_name)

# COMMAND ----------

def update_to_delta_sorted(df,table_name,checkpoint_loc):
  df.writeStream \
  .format("delta") \
  .option("checkpointLocation",checkpoint_loc) \
  .foreachBatch(lambda df,batchId : upsertToDelta(df,batchId,table_name) )\
  .start()
                           

# COMMAND ----------

def copy_to_synapse(delta_table,synapse_table,checkpoint_loc):
   spark.readStream \
        .format("delta") \
        .option("ignoreChanges", "true")\
        .table(delta_table) \
        .writeStream \
        .format("delta") \
        .option("checkpointLocation",checkpoint_loc) \
        .foreachBatch(lambda df,batchId : writeToSQLWarehouse(df,batchId,synapse_table)) \
        .start()

# COMMAND ----------

# MAGIC %md
# MAGIC ##***Staging Layer Utils***

# COMMAND ----------

def parse_table_data(msgs,unacked_msgs,row_insert_tsp_toggle=True,dtl_process=True):
  
  adt_hist_rows = []
  encntr_dx_hist_rows = []
  encntr_er_complnt_hist_rows = []
  encntr_visit_rsn_hist_rows = []
  encntr_nte_hist_rows = []
  
  for msg in msgs:
    
    if msg not in unacked_msgs:
    
      msg_dict = eval(str(msg))
      adt_hist_row = eval(msg_dict["adt_hist"]) if row_insert_tsp_toggle else add_row_insert_tsp_json(eval(msg_dict["adt_hist"]))
      
      adt_hist_rows += [adt_hist_row]
      encntr_er_complnt_hist_rows += [eval(msg_dict["encntr_er_complnt_hist"])]
      encntr_dx_hist_rows += [eval(msg_dict["encntr_dx_hist"])]
      encntr_visit_rsn_hist_rows += [eval(msg_dict["encntr_visit_rsn_hist"])]
      encntr_nte_hist_rows += [eval(msg_dict["encntr_nte_hist"])]
    
  if not dtl_process:
      encntr_dx_hist_rows = [item for sublist in encntr_dx_hist_rows for item in sublist]
      encntr_visit_rsn_hist_rows = [item for sublist in encntr_visit_rsn_hist_rows for item in sublist]
  
  return [adt_hist_rows,encntr_dx_hist_rows,encntr_er_complnt_hist_rows,encntr_visit_rsn_hist_rows, encntr_nte_hist_rows]

# COMMAND ----------

import time

def ack_received_msgs(receiver,msgs):
  unacked_msgs = []
  for msg in msgs:
    try:
      receiver.complete_message(msg)
    except Exception as error:
      unacked_msgs.append(msg)

  return unacked_msgs

# COMMAND ----------

import pandas as pd

def dedup_staging_msgs(staging_msgs):

  dedup_msgs, msg_nms = [],[]

  for msg in staging_msgs:
    msg_dict = eval(str(msg))
    msg_nm = eval(msg_dict["adt_hist"])["msg_nm"]
    if msg_nm not in msg_nms:
      msg_nms.append(msg_nm)
      dedup_msgs.append(msg)

  return dedup_msgs

def write_to_staging(msgs,staging_table_name):
  if msgs:
    df = pd.DataFrame(msgs,columns = ["message_content"])
    df_staging = spark.createDataFrame(df)
    df_staging.write.format("delta").mode("append").saveAsTable(staging_table_name)

def read_from_staging(staging_table_name):
  staging_df = spark.sql("SELECT DISTINCT * FROM {}".format(staging_table_name)).collect()
  staging_msgs = [msg["message_content"] for msg in staging_df]
  dedup_msgs = dedup_staging_msgs(staging_msgs)
  return dedup_msgs

def empty_staging(staging_table_name):
  staging_df = spark.sql("SELECT * FROM {} LIMIT 1".format(staging_table_name))
  empty_df = spark.createDataFrame([],staging_df.schema)
  empty_df.write.format("delta").mode("overwrite").saveAsTable(staging_table_name)

# COMMAND ----------

