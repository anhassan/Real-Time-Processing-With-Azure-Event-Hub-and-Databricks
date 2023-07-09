# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook reads HL7 messages from Event Hub and write data to Delta med_admn Table
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 16/06/2023     AHASSAN2          Initial creation

# COMMAND ----------

# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

# MAGIC %run ../code/hl7/med_admn

# COMMAND ----------

import logging
from pyspark.sql.functions import to_timestamp, from_utc_timestamp, col, lit, concat_ws, current_timestamp, concat, substring_index, length
from opencensus.ext.azure.log_exporter import AzureLogHandler
import json
import os

# COMMAND ----------

checkpoint_path = f'{os.getenv("adls_file_path")}/raw/rltm/checkpoint/epic/med_admn'

target_table = "epic_rltm.med_admn"

# COMMAND ----------

logger = logging.getLogger("epic_rltm_med_admn_hl7_log")
logger.propogate = False
logger.setLevel(logging.DEBUG)
log_template = "Batch Id : {} - Batch Processing Time : {} - Current Timestamp : {}"
app_insights_log_handler = AzureLogHandler(connection_string=application_insights_secret)
logger.addHandler(app_insights_log_handler)

# COMMAND ----------

evh_med_admn_conn_string_listener = 'Endpoint=sb://eh-test-hl7-001.servicebus.windows.net/;SharedAccessKeyName=manage;SharedAccessKey=Cxf2FEUO89ZtpY2MtJxUisXIZsc2+udtn+AEhPpqCwM=;EntityPath=mar_from_epic_hl7'
evh_med_admn_feed_name = "mar_from_epic_hl7"
evh_med_admn_consumer_group = '$Default'

startingEventPosition = {
 "offset": -1,
 "seqNo": -1,
 "enqueuedTime": None,
 "isInclusive": True
}

event_hub_config = {
  "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(evh_med_admn_conn_string_listener),
  "eventshubs.eventHubName": evh_med_admn_feed_name,
  "eventhubs.consumerGroup": evh_med_admn_consumer_group,
  "eventhubs.startingPosition" : json.dumps(startingEventPosition)  
}

# COMMAND ----------

def get_current_user():
  query = 'select current_user' 
  current_user = spark.sql(query)
  return current_user.collect()[0][0]

c_user = get_current_user()

# COMMAND ----------

def execute_transform(streaming_df, c_user):
  
  return streaming_df \
    .select(
      streaming_df.body.cast('string').alias('body'),
      concat_ws("-", col("offset"), col("sequenceNumber")).alias('msg_nm'),
      from_utc_timestamp('enqueuedTime', 'CST').alias('msg_enqueued_tsp')
    ) \
    .withColumn('body', parse_message(col('body'))) \
    .select(
      from_utc_timestamp(to_timestamp('body.msg_tm', 'yyyyMMddHHmmss'), 'CST').alias('msg_tm'),
      'body.msg_typ',
      'body.pat_mrn_id',
      col('body.pat_enc_csn_id').cast('long').alias('pat_enc_csn_id'),
      col('body.ord_id').cast('long').alias('ord_id'),
      'body.ord_stat',
      'body.ord_qnt',
      from_utc_timestamp(to_timestamp('body.ord_strt_tm','yyyyMMddHHmmss'),'CST').alias('ord_strt_tm'),
      from_utc_timestamp(to_timestamp('body.ord_end_tm','yyyyMMddHHmmss'),'CST').alias('ord_end_tm'),
      'body.typ',
      'body.ord_crtr',
      col('body.ord_prvdr_id').cast('long').alias('ord_prvdr_id'),
      'body.ord_typ',
      from_utc_timestamp(to_timestamp('body.admn_tm','yyyyMMddHHmmss'),'CST').alias('admn_tm'),
      'body.med_id',
      'body.med_nm',
      'body.med_brnd_nm',
      'body.dose',
      'body.units',
      'body.admn_prvdr',
      'body.admn_dept',
      'body.mar_actn',
      'body.med_route',
      'msg_nm',
      'msg_enqueued_tsp'
  )

# COMMAND ----------

streaming_df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**event_hub_config) \
  .load()

transformed_streaming_df = execute_transform(streaming_df, c_user)

transformed_streaming_df.writeStream \
  .format('delta') \
  .option("checkpointLocation", checkpoint_path) \
  .toTable(target_table)

# COMMAND ----------

