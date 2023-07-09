# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook reads HL7 messages from Event Hub and write data to Delta Flwsht Table
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 09/01/2022     AHASSAN           Initial creation

# COMMAND ----------

# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

# MAGIC %run ../code/hl7/flwsht

# COMMAND ----------

import logging
from opencensus.ext.azure.log_exporter import AzureLogHandler
from datetime import datetime
import pytz
import os

# COMMAND ----------

checkpoint_path = '{}/raw/rltm/checkpoint/epic/flwsht'.format(os.getenv("adls_file_path"))

flwsht = "epic_rltm.flwsht"


# COMMAND ----------

logger = logging.getLogger("epic_rltm_flwsht_hl7_log")
logger.propogate = False
logger.setLevel(logging.DEBUG)
log_template = "Batch Id : {} - Batch Processing Time : {} - Current Timestamp : {}"
app_insights_log_handler = AzureLogHandler(connection_string=application_insights_secret)
logger.handlers = []
logger.addHandler(app_insights_log_handler)

# COMMAND ----------

def get_current_user():
  query = 'select current_user' 
  current_user = spark.sql(query)
  return current_user.collect()[0][0]

c_user = get_current_user()

# COMMAND ----------

def invoke_all(batch_df, batch_id):
  start_time = datetime.now().astimezone(pytz.timezone('US/Central'))
  
  batch_df.persist()
  
  flwsht_df = batch_df.select(["body","insert_user_id","updt_user_id","msg_nm","enqueued_tsp"]).rdd.map(invoke_flwsht_process).toDF(flwsht_schema).withColumn("flwsht", explode("value")).select("flwsht.*").select(flwsht_columns)
  # flwsht_df = flwsht_df.na.drop(how="all", subset=[col for col in flwsht_df.columns if col not in ["insert_user_id", "msg_nm"]])
  flwsht_df.write.format("delta").mode("append").insertInto(flwsht)
  
   
  batch_df.unpersist()
  
  end_time = datetime.now().astimezone(pytz.timezone('US/Central'))
  logger.info(log_template.format(batch_id, str(end_time - start_time), end_time))
  

# COMMAND ----------

# evh_flwsht_conn_string_listener = 'Endpoint=sb://eh-test-hl7-001.servicebus.windows.net/;SharedAccessKeyName=consume;SharedAccessKey=X24xvGv9UIhXiQUJe3HgMz6FY/ppiTWei+AEhOXMIF0=;EntityPath=flwsht_from_epic_hl7'
# evh_flwsht_feed_name = "flwsht_from_epic_hl7"
# evh_flwsht_consumer_group = '$Default'

# COMMAND ----------

import  json

startingEventPosition = {
 "offset": -1,
 "seqNo": -1,
 "enqueuedTime": None,
 "isInclusive": True
}

# COMMAND ----------

event_hub_config = {
  "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(evh_flwsht_conn_string_listener),
  "eventshubs.eventHubName": evh_flwsht_feed_name,
  "eventhubs.consumerGroup": evh_flwsht_consumer_group,
  "eventhubs.startingPosition" : json.dumps(startingEventPosition)  
  }

# COMMAND ----------

 spark \
  .readStream \
  .format("eventhubs") \
  .options(**event_hub_config) \
  .load() \
  .withColumn("body", col("body").cast("string")) \
  .withColumn("insert_user_id", lit(c_user)) \
  .withColumn("updt_user_id", lit(c_user)) \
  .withColumn("msg_nm",concat_ws("-",col("offset"),col("sequenceNumber"))) \
  .withColumn("enqueued_tsp",col("enqueuedTime")) \
  .select(["body","insert_user_id","updt_user_id","msg_nm","enqueued_tsp"]) \
  .repartition(128) \
  .writeStream \
  .option("checkpointLocation",checkpoint_path) \
  .foreachBatch(lambda batch_df, batch_id: invoke_all(batch_df, batch_id)) \
  .start()
