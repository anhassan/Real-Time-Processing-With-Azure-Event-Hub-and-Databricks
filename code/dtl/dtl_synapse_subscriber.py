# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook reads messages from Service Bus Topic and write data to Synapse ADT Hist Tables
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 09/01/2022     AHASSAN2          Initial creation


# COMMAND ----------

# MAGIC %run ./adt_dtl

# COMMAND ----------

from azure.servicebus import ServiceBusClient, ServiceBusMessage, AutoLockRenewer
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

TOPIC_RECEIVE_CONN_STR = "Endpoint=sb://sb-test-hl7-001.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=Lk3Wpzvrz2BgnvefIjeMKlR5gYBjjk0qLOqNEYnd2i0=;EntityPath=parsed_hl7_adt_from_healthconnect"
TOPIC_NAME= "parsed_hl7_adt_from_healthconnect"
SUBSCRIPTION_NAME = "parsed_hl7_adt_to_synapse_dtl"

# COMMAND ----------

detail_table_names = {
  "adt_dtl" : "epic_rltm.adt_dtl",
  "encntr_dx" : "epic_rltm.encntr_dx",
  "encntr_er_complnt" : "epic_rltm.encntr_er_complnt",
  "encntr_visit_rsn" : "epic_rltm.encntr_visit_rsn",
  "encntr_nte" : "epic_rltm.encntr_nte"
}

# COMMAND ----------

import json

def to_timestamp_cols(df,timestamp_cols):
  for column in timestamp_cols:
    if column in df.columns:
      df = df.withColumn(column,to_timestamp(col(column)))
  return df

def jsonify(input_str):
  return json.loads(input_str.replace("\'", "\"")\
                             .replace("None","\"\""))
  
def drop_na(df,cols=["pat_enc_csn_id","msg_src"]):
  df = df.na.drop(subset=cols)
  return df

# COMMAND ----------

import json
from datetime import datetime
import time

def persist_dtl_data_from_subscription_time_batching_synapse(topic_conn_str,topic_name,subscription_name,detail_table_names):
  servicebus_client_topic_receive = ServiceBusClient.from_connection_string(conn_str=topic_conn_str,logging_enable=True)
  
  adt_hist_rows = []
  encntr_dx_hist_rows = []
  encntr_er_complnt_hist_rows = []
  encntr_visit_rsn_hist_rows = []
  msg_ack_list = []
  
  batch_size = 1000
  batch_id = 0
  timestamp_cols = ["msg_tm", "birth_dt","death_dt","hosp_admsn_tm","hosp_disch_tm","adt_arvl_tm","row_insert_tsp"]
  lock_renewal = AutoLockRenewer()
 
  
  with servicebus_client_topic_receive:
    receiver = servicebus_client_topic_receive.get_subscription_receiver(topic_name=topic_name,subscription_name=subscription_name)
    with receiver:
       while 1==1:
        t_end = time.time() + 60
        for msg in receiver:
          msg_ack_list += [msg]
          msg_dict = eval(str(msg))
          adt_hist_rows += [add_row_insert_tsp_json(eval(msg_dict["adt_hist"]))]
          encntr_dx_hist_rows += [eval(msg_dict["encntr_dx_hist"])]
          encntr_er_complnt_hist_rows += [eval(msg_dict["encntr_er_complnt_hist"])]
          encntr_visit_rsn_hist_rows += [eval(msg_dict["encntr_visit_rsn_hist"])]
          receiver.complete_message(msg)
          if time.time() > t_end:
            break

        batch_id +=1 
        print("batch id ", batch_id)

        adt_hist_batch = drop_na(to_timestamp_cols(spark.createDataFrame(adt_hist_rows,adt_hist_schema)\
                              .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast(DecimalType(18,0)))\
                              .withColumn("hsp_acct_id",col("hsp_acct_id").cast(DecimalType(18,0))),timestamp_cols))
                                   
          
        encntr_dx_hist_batch = drop_na(to_timestamp_cols(spark.createDataFrame(encntr_dx_hist_rows,encntr_dx_hist_schema)\
                                        .withColumn("row_insert_tsp", add_row_insert_tsp())\
                                        .withColumn("parse",explode(col("value")))\
                                        .select(["parse.*","row_insert_tsp"])\
                                        .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
                                        ,timestamp_cols))
          
        encntr_er_complnt_hist_batch = drop_na(to_timestamp_cols(spark.createDataFrame(encntr_er_complnt_hist_rows,encntr_er_complnt_hist_schema)\
                                                              .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
                                                              .withColumn("row_insert_tsp", add_row_insert_tsp()),timestamp_cols))
          
          
        encntr_visit_rsn_hist_batch = drop_na(to_timestamp_cols(spark.createDataFrame(encntr_visit_rsn_hist_rows,encntr_visit_rsn_hist_schema)\
                                                  .withColumn("row_insert_tsp", add_row_insert_tsp())\
                                                  .withColumn("parse",explode(col("value")))\
                                                  .select(["parse.*","row_insert_tsp"])\
                                                  .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
                                                  , timestamp_cols))
          
          
        print(datetime.now())
        captureChangeDataSynapse(adt_hist_batch, detail_table_names["adt_dtl"])
        captureChangeDataChildSynapseOptimizedDelete(encntr_dx_hist_batch, ["dx_icd_cd","dx_nm"], detail_table_names["encntr_dx"])
        captureChangeDataChildSynapseOptimizedDelete(encntr_er_complnt_hist_batch, ["er_complnt"], detail_table_names["encntr_er_complnt"])
        captureChangeDataChildSynapseOptimizedDelete(encntr_visit_rsn_hist_batch, ["encntr_rsn"], detail_table_names["encntr_visit_rsn"])
        print(datetime.now())
          
          
        adt_hist_rows = []
        encntr_dx_hist_rows = []
        encntr_er_complnt_hist_rows = []
        encntr_visit_rsn_hist_rows = []

        # for msg in msg_ack_list:
        #   receiver.complete_message(msg)

        msg_ack_list = []
        

# COMMAND ----------

import json
from datetime import datetime
import time

def persist_dtl_data_from_subscription_synapse(topic_conn_str,topic_name,subscription_name,detail_table_names):
  servicebus_client_topic_receive = ServiceBusClient.from_connection_string(conn_str=topic_conn_str,logging_enable=True)
  
  adt_hist_rows = []
  encntr_dx_hist_rows = []
  encntr_er_complnt_hist_rows = []
  encntr_visit_rsn_hist_rows = []
  msg_ack_list = []
  
  batch_size = 1000
  batch_id = 0
  timestamp_cols = ["msg_tm", "birth_dt","death_dt","hosp_admsn_tm","hosp_disch_tm","adt_arvl_tm","row_insert_tsp"]
  lock_renewal = AutoLockRenewer()
  
  with servicebus_client_topic_receive:
    receiver = servicebus_client_topic_receive.get_subscription_receiver(topic_name=topic_name,subscription_name=subscription_name)
    with receiver:
       while 1==1:
        start_time = time.time()
        received_msgs = receiver.receive_messages(max_message_count=batch_size,max_wait_time=5)
        num_msgs_received = 0
        for msg in received_msgs:
          num_msgs_received +=1
          msg_dict = eval(str(msg))
          adt_hist_rows += [add_row_insert_tsp_json(eval(msg_dict["adt_hist"]))]
          encntr_dx_hist_rows += [eval(msg_dict["encntr_dx_hist"])]
          encntr_er_complnt_hist_rows += [eval(msg_dict["encntr_er_complnt_hist"])]
          encntr_visit_rsn_hist_rows += [eval(msg_dict["encntr_visit_rsn_hist"])]
          receiver.complete_message(msg)
        
        end_time = time.time()
        print("Read {} msgs in Time : {}".format(num_msgs_received,end_time - start_time))
        
        batch_id +=1 
        print("batch id ", batch_id)

        adt_hist_batch = drop_na(to_timestamp_cols(spark.createDataFrame(adt_hist_rows,adt_hist_schema)\
                              .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast(DecimalType(18,0)))\
                              .withColumn("hsp_acct_id",col("hsp_acct_id").cast(DecimalType(18,0))),timestamp_cols))
                                   
          
        encntr_dx_hist_batch = drop_na(to_timestamp_cols(spark.createDataFrame(encntr_dx_hist_rows,encntr_dx_hist_schema)\
                                        .withColumn("row_insert_tsp", add_row_insert_tsp())\
                                        .withColumn("parse",explode(col("value")))\
                                        .select(["parse.*","row_insert_tsp"])\
                                        .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
                                        ,timestamp_cols))
          
        encntr_er_complnt_hist_batch = drop_na(to_timestamp_cols(spark.createDataFrame(encntr_er_complnt_hist_rows,encntr_er_complnt_hist_schema)\
                                                              .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
                                                              .withColumn("row_insert_tsp", add_row_insert_tsp()), timestamp_cols))
          
          
        encntr_visit_rsn_hist_batch = drop_na(to_timestamp_cols(spark.createDataFrame(encntr_visit_rsn_hist_rows,encntr_visit_rsn_hist_schema)\
                                                  .withColumn("row_insert_tsp", add_row_insert_tsp())\
                                                  .withColumn("parse",explode(col("value")))\
                                                  .select(["parse.*","row_insert_tsp"])\
                                                  .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
                                                  , timestamp_cols))
          
        print("MERGE STARTED........")  
        print(datetime.now())
        captureChangeDataSynapse(adt_hist_batch, detail_table_names["adt_dtl"])
        #print("1111111")
        captureChangeDataChildSynapseOptimizedDelete(encntr_dx_hist_batch, ["dx_icd_cd","dx_nm"], detail_table_names["encntr_dx"])
        captureChangeDataChildSynapseOptimizedDelete(encntr_er_complnt_hist_batch, ["er_complnt"], detail_table_names["encntr_er_complnt"])
        captureChangeDataChildSynapseOptimizedDelete(encntr_visit_rsn_hist_batch, ["encntr_rsn"], detail_table_names["encntr_visit_rsn"])
        print(datetime.now())
        print("MERGE ENDED...........")
          
          
        adt_hist_rows = []
        encntr_dx_hist_rows = []
        encntr_er_complnt_hist_rows = []
        encntr_visit_rsn_hist_rows = []

        # for msg in msg_ack_list:
        #   receiver.complete_message(msg)

        msg_ack_list = []
        

# COMMAND ----------

import json
from datetime import datetime
import time
import traceback

def persist_dtl_data_from_subscription_synapse_with_staging(topic_conn_str,topic_name,subscription_name,detail_table_names, staging_table_name):
  servicebus_client_topic_receive = ServiceBusClient.from_connection_string(conn_str=topic_conn_str,logging_enable=True,uamqp_transport=True)
  
  adt_hist_rows = []
  encntr_dx_hist_rows = []
  encntr_er_complnt_hist_rows = []
  encntr_visit_rsn_hist_rows = []
  encntr_nte_hist_rows = []
  staging_msgs = []
  unacked_msgs = []
  
  batch_size = 1000
  batch_id = 0
  timestamp_cols = ["msg_tm", "birth_dt","death_dt","hosp_admsn_tm","hosp_disch_tm","adt_arvl_tm","row_insert_tsp", "msg_enqueued_tsp"]
  lock_renewal = AutoLockRenewer()
  
  with servicebus_client_topic_receive:
    receiver = servicebus_client_topic_receive.get_subscription_receiver(topic_name=topic_name,subscription_name=subscription_name)
    with receiver:
       while 1==1:
        staged_msgs = read_from_staging(staging_table_name)
        print("Read messages from Staging Layer...")      
        
        if not staged_msgs:
          start_time = time.time()
          received_msgs = receiver.receive_messages(max_message_count=batch_size, max_wait_time=5)
          num_msgs_received = len(received_msgs)
          
          for msg in received_msgs:
              staging_msgs.append([str(msg)])
          
          end_time = time.time()
          print("Read {} msgs in Time : {} ".format(num_msgs_received, end_time - start_time))
          
          write_to_staging(staging_msgs,staging_table_name)
          print("Staged {} messages in staging layer - table name : {}".format(num_msgs_received, staging_table_name))
          
          unacked_msgs = ack_received_msgs(receiver, received_msgs)
          print("Acknowledged {} messages".format(num_msgs_received))

          staged_msgs = [msg for msg_list in staging_msgs for msg in msg_list]

        try:
          batch_id +=1 
          print("batch id ", batch_id)
          
          if staged_msgs:
            
            [adt_hist_rows,encntr_dx_hist_rows,encntr_er_complnt_hist_rows,encntr_visit_rsn_hist_rows, encntr_nte_hist_rows] = parse_table_data(staged_msgs,unacked_msgs, False, True)
          
            adt_hist_batch = drop_na(to_timestamp_cols(spark.createDataFrame(adt_hist_rows,adt_hist_schema)\
                                  .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast(DecimalType(18,0)))\
                                  .withColumn("hsp_acct_id",col("hsp_acct_id").cast(DecimalType(18,0))),timestamp_cols))
                                      
              
            encntr_dx_hist_batch = drop_na(to_timestamp_cols(spark.createDataFrame(encntr_dx_hist_rows,encntr_dx_hist_schema)\
                                            .withColumn("row_insert_tsp", add_row_insert_tsp())\
                                            .withColumn("parse",explode(col("value")))\
                                            .select(["parse.*","row_insert_tsp"])\
                                            .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
                                            ,timestamp_cols))
              
            encntr_er_complnt_hist_batch = drop_na(to_timestamp_cols(spark.createDataFrame(encntr_er_complnt_hist_rows,encntr_er_complnt_hist_schema)\
                                                                  .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
                                                                  .withColumn("row_insert_tsp", add_row_insert_tsp()), timestamp_cols))
              
              
            encntr_visit_rsn_hist_batch = drop_na(to_timestamp_cols(spark.createDataFrame(encntr_visit_rsn_hist_rows,encntr_visit_rsn_hist_schema)\
                                                      .withColumn("row_insert_tsp", add_row_insert_tsp())\
                                                      .withColumn("parse",explode(col("value")))\
                                                      .select(["parse.*","row_insert_tsp"])\
                                                      .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
                                                      , timestamp_cols))
            
            encntr_nte_hist_batch = drop_na(to_timestamp_cols(spark.createDataFrame(encntr_nte_hist_rows,encntr_nte_hist_schema),timestamp_cols))\
                                                            .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
                                                            .withColumn("row_insert_tsp", add_row_insert_tsp())
              
            print("MERGE STARTED........")  
            print(datetime.now())
            captureChangeDataSynapse(adt_hist_batch, detail_table_names["adt_dtl"])
            #print("1111111")
            captureChangeDataChildSynapseOptimizedDelete(encntr_dx_hist_batch, ["dx_icd_cd","dx_nm"], detail_table_names["encntr_dx"])
            captureChangeDataChildSynapseOptimizedDelete(encntr_er_complnt_hist_batch, ["er_complnt"], detail_table_names["encntr_er_complnt"])
            captureChangeDataChildSynapseOptimizedDelete(encntr_visit_rsn_hist_batch, ["encntr_rsn"], detail_table_names["encntr_visit_rsn"])
            captureChangeDataChildSynapseOptimizedDelete(encntr_nte_hist_batch, ["nte_txt", "nte_typ"], detail_table_names["encntr_nte"])
            print(datetime.now())
            print("MERGE ENDED...........")
            
            adt_hist_rows = []
            encntr_dx_hist_rows = []
            encntr_er_complnt_hist_rows = []
            encntr_visit_rsn_hist_rows = []
            encntr_nte_hist_rows = []
                    
            empty_staging(staging_table_name)
            print("Emptied Staging Layer...")
            staging_msgs = []
          
        except Exception as error:
          print(traceback.print_exc())
          print("Retrying batch : {}".format(batch_id))
          batch_id = batch_id - 1

# COMMAND ----------

