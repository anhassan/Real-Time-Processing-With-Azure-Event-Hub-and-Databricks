# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook consumes messages from Service Bus Topic and writes the data to the ADT Delta Hist Tables in Databricks
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 08/30/2022     AHASSAN2           Initial Creation
# 03/09/2023     AHASSAN2           Added ROW_UPDT_TSP and UPDATE_USER_ID fields
# 03/14/2023     AHASSAN2           Added parallel read from source logic
# 03/27/2023     AHASSAN2           Added staging layer logic

# COMMAND ----------

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from pyspark.sql.functions import to_timestamp,lit,col
from pyspark.sql.types import *
from datetime import datetime
import time

# COMMAND ----------

# MAGIC %run ../utils/utilities

# COMMAND ----------

# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

SUB_1_NAME = "parsed_hl7_adt_to_delta_hist"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Subscriber 1

# COMMAND ----------

adt_hist_schema_src = StructType(
    [
        StructField("msg_typ", StringType(), True),
        StructField("msg_src", StringType(), True),
        StructField("trigger_evnt", StringType(), True),
        StructField("msg_tm", StringType(), True),
        StructField("msg_nm", StringType(), True),
        StructField("bod_id", StringType(), True),
        StructField("pat_mrn_id", StringType(), True),
        StructField("pat_enc_csn_id", LongType(), True),
        StructField("birth_date", StringType(), True),
        StructField("death_date", StringType(), True),
        StructField("pat_class_abbr", StringType(), True),
        StructField("hosp_admsn_time", StringType(), True),
        StructField("hosp_disch_time", StringType(), True),
        StructField("department_abbr", StringType(), True),
        StructField("loc_abbr", StringType(), True),
        StructField("room_nm", StringType(), True),
        StructField("bed_label", StringType(), True),
        StructField("bed_status", StringType(), True),
        StructField("sex_abbr", StringType(), True),
        StructField("means_of_arrv_abbr", StringType(), True),
        StructField("acuity_level_abbr", StringType(), True),
        StructField("ed_disposition_abbr", StringType(), True),
        StructField("disch_disp_abbr", StringType(), True),
        StructField("adt_arrival_time", StringType(), True),
        StructField("hsp_account_id", LongType(), True),  # Long
        StructField("accommodation_abbr", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("insert_user_id", StringType(), True),     
    ]
)

encntr_dx_hist_schema_src =StructType(
        [
            StructField("msg_src", StringType(), True),
            StructField("msg_tm", StringType(), True),
            StructField("pat_enc_csn_id", LongType(), True),
            StructField("dx_icd_cd", StringType(), True),
            StructField("dx_name", StringType(), True),
            StructField("dx_code_type", StringType(), True),
            StructField("insert_user_id", StringType(), True),
            
        ]
    )

encntr_er_complnt_hist_schema_src = StructType(
    [
        StructField("msg_src", StringType(), True),
        StructField("msg_tm", StringType(), True),
        StructField("pat_enc_csn_id", LongType(), True),
        StructField("er_complaint", StringType(), True),
        StructField("insert_user_id", StringType(), True),
        
    ]
)

encntr_visit_rsn_hist_schema_src = StructType(
        [
            StructField("msg_src", StringType(), True),
            StructField("msg_tm", StringType(), True),
            StructField("pat_enc_csn_id", LongType(), True),
            StructField("encntr_rsn", StringType(), True),
            StructField("ed_triage_nte", StringType(), True),
            StructField("insert_user_id", StringType(), True),
          
        ]
    )

# COMMAND ----------

adt_hist_columns = ['msg_typ','msg_src','trigger_evnt','msg_tm','msg_nm','bod_id','pat_mrn_id','pat_enc_csn_id','birth_date','death_date','pat_class_abbr',
                    'hosp_admsn_time','hosp_disch_time','department_abbr','loc_abbr','room_nm','bed_label','bed_status','sex_abbr','means_of_arrv_abbr',
                    'acuity_level_abbr','ed_disposition_abbr','disch_disp_abbr','adt_arrival_time','hsp_account_id','accommodation_abbr','user_id',
                    'row_insert_tsp','row_updt_tsp','insert_user_id','update_user_id']

encntr_dx_hist_columns = ["msg_src","msg_tm","pat_enc_csn_id","dx_icd_cd","dx_name","dx_code_type","row_insert_tsp","row_updt_tsp","insert_user_id","update_user_id"]
encntr_er_complnt_hist_columns = ["msg_src","msg_tm","pat_enc_csn_id","er_complaint","row_insert_tsp","row_updt_tsp","insert_user_id","update_user_id"]

encntr_visit_rsn_columns=["msg_src","msg_tm","pat_enc_csn_id","encntr_rsn","row_insert_tsp","row_updt_tsp","insert_user_id","update_user_id", "ed_triage_nte"]

# COMMAND ----------

def insert_into_adt_hist(adt_hist_rows, adt_hist_schema_src, adt_hist_table):
 
  spark.createDataFrame(adt_hist_rows, adt_hist_schema_src).withColumn("birth_date", to_timestamp("birth_date")) \
     .withColumn("death_date", to_timestamp("death_date")) \
     .withColumn("msg_tm", to_timestamp("msg_tm")) \
     .withColumn("hosp_admsn_time", to_timestamp("hosp_admsn_time")) \
     .withColumn("hosp_disch_time", to_timestamp("hosp_disch_time")) \
     .withColumn("adt_arrival_time", to_timestamp("adt_arrival_time")) \
     .withColumn("row_insert_tsp", lit(datetime.now())) \
     .withColumn("row_updt_tsp",col("row_insert_tsp"))\
     .withColumn("update_user_id",col("insert_user_id"))\
     .withColumn("pat_enc_csn_id", col("pat_enc_csn_id").cast(DecimalType(18, 0))) \
     .withColumn("hsp_account_id", col("hsp_account_id").cast(DecimalType(18, 0))) \
     .select(adt_hist_columns) \
     .na.drop(subset=["msg_src","pat_enc_csn_id"]) \
     .write.format("delta").mode("append").insertInto(adt_hist_table)
  
def insert_into_encntr_dx_hist(encntr_dx_hist_rows, encntr_dx_hist_schema_src, encntr_dx_hist_table):

  spark.createDataFrame(encntr_dx_hist_rows, encntr_dx_hist_schema_src) \
    .withColumn("row_insert_tsp", lit(datetime.now())) \
    .withColumn("row_updt_tsp",col("row_insert_tsp"))\
    .withColumn("update_user_id",col("insert_user_id"))\
    .withColumn("pat_enc_csn_id", col("pat_enc_csn_id").cast(DecimalType(18, 0))) \
    .select(encntr_dx_hist_columns) \
    .na.drop(subset=["msg_src","pat_enc_csn_id"]) \
    .write.format("delta").mode("append").insertInto(encntr_dx_hist_table) 
  
def insert_into_encntr_er_complnt_hist(encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src, encntr_er_complnt_hist_table):

  spark.createDataFrame(encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src) \
      .withColumn("msg_tm", to_timestamp("msg_tm"))\
      .withColumn("row_insert_tsp", lit(datetime.now())) \
      .withColumn("row_updt_tsp",col("row_insert_tsp"))\
      .withColumn("update_user_id",col("insert_user_id"))\
      .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast(DecimalType(18,0))) \
      .select(encntr_er_complnt_hist_columns) \
      .na.drop(subset=["msg_src","pat_enc_csn_id"]) \
      .write.format("delta").mode("append").insertInto(encntr_er_complnt_hist_table)
  
def insert_into_encntr_visit_rsn_hist(encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src, encntr_visit_rsn_hist_table):

  spark.createDataFrame(encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src) \
      .withColumn("row_insert_tsp", lit(datetime.now())) \
      .withColumn("row_updt_tsp",col("row_insert_tsp"))\
      .withColumn("update_user_id",col("insert_user_id"))\
      .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast(DecimalType(18,0))) \
      .select(encntr_visit_rsn_columns) \
      .na.drop(subset=["msg_src","pat_enc_csn_id"]) \
      .write.format("delta").mode("append").insertInto(encntr_visit_rsn_hist_table)
  

# COMMAND ----------

def sub_to_delta(service_bus_topic_receive_conn_str, service_bus_topic_name, sub_name, table_args_src):
  
  servicebus_client_topic_receive = ServiceBusClient.from_connection_string(service_bus_topic_receive_conn_str, logging_enable=True)

  adt_hist_rows = []
  encntr_dx_hist_rows = []
  encntr_er_complnt_hist_rows = []
  encntr_visit_rsn_hist_rows = []
  batch_size = 500
  
  with servicebus_client_topic_receive:
      receiver = servicebus_client_topic_receive.get_subscription_receiver(topic_name=service_bus_topic_name, subscription_name=sub_name)#, max_wait_time=5
      with receiver:
        while 1==1:
          start_time = time.time()
          received_msgs = receiver.receive_messages(max_message_count=batch_size, max_wait_time=5)
        
          for msg in received_msgs:
              msg_dict = eval(str(msg))  
              adt_hist_rows.append(eval(msg_dict["adt_hist"]))

              if msg_dict["encntr_dx_hist"] is not None:
                msg_dict_encntr_dx_hist = eval(msg_dict["encntr_dx_hist"])
                for idx in range(len(msg_dict_encntr_dx_hist)):
                    encntr_dx_hist_rows.append(msg_dict_encntr_dx_hist[idx])

              if msg_dict["encntr_er_complnt_hist"] is not None:
                encntr_er_complnt_hist_rows.append(eval(msg_dict["encntr_er_complnt_hist"]))

              if msg_dict["encntr_visit_rsn_hist"] is not None:
                msg_dict_encntr_visit_rsn_hist = eval(msg_dict["encntr_visit_rsn_hist"])
                for idx in range(len(msg_dict_encntr_visit_rsn_hist)):
                    encntr_visit_rsn_hist_rows.append(msg_dict_encntr_visit_rsn_hist[idx])
                    
              receiver.complete_message(msg)
                
          end_time = time.time()
          print("Read {} msgs in Time : {} ".format(str(len(received_msgs)),end_time-start_time))  
              
          print("INSERT STARTED.....")
          print(datetime.now())
              
          insert_into_adt_hist(
            adt_hist_rows, adt_hist_schema_src=table_args_src["adt_hist"]["schema"], 
            adt_hist_table=table_args_src["adt_hist"]["table"]
          )
          adt_hist_rows = []

          insert_into_encntr_dx_hist(
            encntr_dx_hist_rows, encntr_dx_hist_schema_src=table_args_src["encntr_dx_hist"]["schema"], 
            encntr_dx_hist_table=table_args_src["encntr_dx_hist"]["table"]
          )
          encntr_dx_hist_rows = []
    
          insert_into_encntr_er_complnt_hist(
            encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src=table_args_src["encntr_er_complnt_hist"]["schema"], 
            encntr_er_complnt_hist_table=table_args_src["encntr_er_complnt_hist"]["table"]
          )
          encntr_er_complnt_hist_rows = []
        
          insert_into_encntr_visit_rsn_hist(
            encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src=table_args_src["encntr_visit_rsn_hist"]["schema"], 
            encntr_visit_rsn_hist_table=table_args_src["encntr_visit_rsn_hist"]["table"]
          )
          encntr_visit_rsn_hist_rows = []
            
          print(datetime.now())
          print("INSERT ENDED.......")
                
table_args_src = {
  "adt_hist": {
    "table": "epic_rltm.adt_hist_mult_cnsmr",
    "schema": adt_hist_schema_src
  },
  "encntr_dx_hist": {
    "table": "epic_rltm.encntr_dx_hist_mult_cnsmr",
    "schema": encntr_dx_hist_schema_src
  },
  "encntr_er_complnt_hist": {
    "table": "epic_rltm.encntr_er_complnt_hist_mult_cnsmr",
    "schema": encntr_er_complnt_hist_schema_src
  },
  "encntr_visit_rsn_hist": {
    "table": "epic_rltm.encntr_visit_rsn_hist_mult_cnsmr",
    "schema": encntr_visit_rsn_hist_schema_src
  }
}


#sub_to_delta(service_bus_topic_receive_conn_str, service_bus_topic_name, SUB_1_NAME,table_args_src)

# COMMAND ----------

def sub_to_delta_with_staging(service_bus_topic_receive_conn_str, service_bus_topic_name, sub_name, table_args_src):
  
  servicebus_client_topic_receive = ServiceBusClient.from_connection_string(service_bus_topic_receive_conn_str, logging_enable=True)

  adt_hist_rows = []
  encntr_dx_hist_rows = []
  encntr_er_complnt_hist_rows = []
  encntr_visit_rsn_hist_rows = []
  staging_msgs = []
  staging_table_name = "epic_rltm.adt_hist_mult_cnsmr_staging"
  batch_size = 500
  batch_id = 0
  
  with servicebus_client_topic_receive:
      receiver = servicebus_client_topic_receive.get_subscription_receiver(topic_name=service_bus_topic_name, subscription_name=sub_name)#, max_wait_time=5
      with receiver:
        while 1==1:
          start_time = time.time()
          received_msgs = receiver.receive_messages(max_message_count=batch_size, max_wait_time=5)
          num_msgs_received = len(received_msgs)
        
          for msg in received_msgs:
            staging_msgs.append((str(msg)))
          
          end_time = time.time()
          print("Read {} msgs in Time : {} ".format(num_msgs_received, end_time - start_time))
        
          write_to_staging(staging_msgs, staging_table_name)
          print("Staged {} messages in staging layer - table name : {}".format(num_msgs_received, staging_table_name))
        
          unacked_msgs = ack_received_msgs(receiver, received_msgs)
          print("Acknowledged {} messages".format(num_msgs_received - len(unacked_msgs)))
          
          try:
            batch_id +=1 
            print("batch id ", batch_id)
            
            staged_msgs = read_from_staging(staging_table_name)
            print("Read messages from Staging Layer...")
                        
            if staged_msgs:
              [adt_hist_rows,encntr_dx_hist_rows,encntr_er_complnt_hist_rows,encntr_visit_rsn_hist_rows] = parse_table_data(staged_msgs, True, False)
            
              print("INSERT STARTED.....")
              print(datetime.now())
                  
              insert_into_adt_hist(
                adt_hist_rows, adt_hist_schema_src=table_args_src["adt_hist"]["schema"], 
                adt_hist_table=table_args_src["adt_hist"]["table"]
              )
              adt_hist_rows = []
    
              insert_into_encntr_dx_hist(
                encntr_dx_hist_rows, encntr_dx_hist_schema_src=table_args_src["encntr_dx_hist"]["schema"], 
                encntr_dx_hist_table=table_args_src["encntr_dx_hist"]["table"]
              )
              encntr_dx_hist_rows = []
        
              insert_into_encntr_er_complnt_hist(
                encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src=table_args_src["encntr_er_complnt_hist"]["schema"], 
                encntr_er_complnt_hist_table=table_args_src["encntr_er_complnt_hist"]["table"]
              )
              encntr_er_complnt_hist_rows = []
            
              insert_into_encntr_visit_rsn_hist(
                encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src=table_args_src["encntr_visit_rsn_hist"]["schema"], 
                encntr_visit_rsn_hist_table=table_args_src["encntr_visit_rsn_hist"]["table"]
              )
              encntr_visit_rsn_hist_rows = []
                
              print(datetime.now())
              print("INSERT ENDED.......")
              
              empty_staging(staging_table_name)
              print("Emptied Staging Layer...")
              staging_msgs = []
          
          except Exception as error:
            print("Retrying batch : {}".format(batch_id))
            batch_id = batch_id - 1
            
                
table_args_src = {
  "adt_hist": {
    "table": "epic_rltm.adt_hist_mult_cnsmr",
    "schema": adt_hist_schema_src
  },
  "encntr_dx_hist": {
    "table": "epic_rltm.encntr_dx_hist_mult_cnsmr",
    "schema": encntr_dx_hist_schema_src
  },
  "encntr_er_complnt_hist": {
    "table": "epic_rltm.encntr_er_complnt_hist_mult_cnsmr",
    "schema": encntr_er_complnt_hist_schema_src
  },
  "encntr_visit_rsn_hist": {
    "table": "epic_rltm.encntr_visit_rsn_hist_mult_cnsmr",
    "schema": encntr_visit_rsn_hist_schema_src
  }
}


sub_to_delta_with_staging(service_bus_topic_receive_conn_str, service_bus_topic_name, SUB_1_NAME, table_args_src)