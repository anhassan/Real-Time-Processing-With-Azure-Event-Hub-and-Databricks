# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook reads messages from Service Bus Topic and write data to Synapse ADT Hist Tables
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 03/14/2023     AHASSAN2          Added parallel read from source logic



# COMMAND ----------

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType,DecimalType,StringType,IntegerType
import time


# COMMAND ----------

# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

# MAGIC %run ../../../etl/data_engineering/commons/utilities

# COMMAND ----------

# MAGIC %run ../utils/utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ####Connection Details for Topic and Subscriber

# COMMAND ----------

TOPIC_LISTEN_CONN_STRING = service_bus_topic_receive_conn_str

TOPIC_NAME="parsed_hl7_adt_from_healthconnect"

SUBSCRIPTION_3_NAME = "parsed_hl7_adt_to_synapse_hist"

servicebus_client_topic_receive = ServiceBusClient.from_connection_string(conn_str=TOPIC_LISTEN_CONN_STRING, logging_enable=True)


# COMMAND ----------

adt_hist_columns = ['msg_typ','msg_src','trigger_evnt','msg_tm','msg_nm','bod_id','pat_mrn_id','pat_enc_csn_id','birth_date','death_date','pat_class_abbr','hosp_admsn_time','hosp_disch_time',
                    'department_abbr','loc_abbr','room_nm','bed_label','bed_status','sex_abbr','means_of_arrv_abbr','acuity_level_abbr','ed_disposition_abbr',
                    'disch_disp_abbr','adt_arrival_time','hsp_account_id','accommodation_abbr','user_id','row_insert_tsp','row_updt_tsp','insert_user_id','update_user_id']

encntr_dx_hist_columns = ["msg_src","msg_tm","pat_enc_csn_id","dx_icd_cd","dx_name","dx_code_type","row_insert_tsp","row_updt_tsp","insert_user_id","update_user_id"]


encntr_er_complnt_hist_columns = ["msg_src","msg_tm","pat_enc_csn_id","er_complaint","row_insert_tsp","row_updt_tsp","insert_user_id","update_user_id"]

encntr_visit_rsn_columns=["msg_src","msg_tm","pat_enc_csn_id","encntr_rsn","row_insert_tsp","row_updt_tsp","insert_user_id","update_user_id", "ed_triage_nte"]


# COMMAND ----------

adt_hist_schema = StructType(
    [
        StructField("msg_typ", StringType(), True),
        StructField("msg_src", StringType(), True),
        StructField("trigger_evnt", StringType(), True),
        StructField("msg_tm", StringType(), True),#         StructField("msg_tm", TimestampType(), True),
        StructField("msg_nm", StringType(), True),
        StructField("bod_id", StringType(), True),
        StructField("pat_mrn_id", StringType(), True),
        StructField("pat_enc_csn_id", LongType(), True),
        StructField("birth_date", StringType(), True),#         StructField("birth_date", TimestampType(), True),
        StructField("death_date", StringType(), True),#         StructField("death_date", TimestampType(), True),
        StructField("pat_class_abbr", StringType(), True),
        StructField("hosp_admsn_time", StringType(), True),#         StructField("hosp_admsn_time", TimestampType(), True),
        StructField("hosp_disch_time", StringType(), True),#         StructField("hosp_disch_time", TimestampType(), True),
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
        StructField("adt_arrival_time", StringType(), True),#         StructField("adt_arrival_time", TimestampType(), True),
        StructField("hsp_account_id", LongType(), True),  # Long
        StructField("accommodation_abbr", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("row_insert_tsp", StringType(), True),#         StructField("row_insert_tsp", TimestampType(), True),
        StructField("insert_user_id", StringType(), True),     
    ]
)


encntr_dx_hist_schema =StructType(
        [
            StructField("msg_src", StringType(), True),
            StructField("msg_tm", StringType(), True),#             StructField("msg_tm", TimestampType(), True),
            StructField("pat_enc_csn_id", LongType(), True),
            StructField("dx_icd_cd", StringType(), True),
            StructField("dx_name", StringType(), True),
            StructField("dx_code_type", StringType(), True),
            StructField("insert_user_id", StringType(), True),            
        ]
    )


encntr_er_complnt_hist_schema = StructType(
    [
        StructField("msg_src", StringType(), True),
        StructField("msg_tm", StringType(), True),#         StructField("msg_tm", TimestampType(), True),
        StructField("pat_enc_csn_id", LongType(), True),
        StructField("er_complaint", StringType(), True),
        StructField("insert_user_id", StringType(), True),
        
    ]
)



encntr_visit_rsn_hist_schema = StructType(
        [
            StructField("msg_src", StringType(), True),
            StructField("msg_tm", StringType(), True),#             StructField("msg_tm", TimestampType(), True),
            StructField("pat_enc_csn_id", LongType(), True),
            StructField("encntr_rsn", StringType(), True),
            StructField("ed_triage_nte", StringType(), True),
            StructField("insert_user_id", StringType(), True),

        ]
    )


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

# MAGIC %md
# MAGIC ####Receive data from Subscriber and Write it to Synapse

# COMMAND ----------


# adt_hist_rows = []
# encntr_dx_hist_rows = []
# encntr_er_complnt_hist_rows = []
# encntr_visit_rsn_hist_rows = []

# batch_size = 500
# timestamp_cols = ["msg_tm", "birth_date","death_date","hosp_admsn_time","hosp_disch_time","adt_arrival_time"]


# with servicebus_client_topic_receive:
#     receiver = servicebus_client_topic_receive.get_subscription_receiver(topic_name=TOPIC_NAME, subscription_name=SUBSCRIPTION_3_NAME)
#     with receiver:
#       while 1==1:
#         start_time = time.time()
#         received_msgs = receiver.receive_messages(max_message_count=batch_size, max_wait_time=5)
        
#         for msg in received_msgs:
#             msg_dict = eval(str(msg))  
#             adt_hist_rows.append(eval(msg_dict["adt_hist"]))
            
#             msg_dict_encntr_dx_hist = eval(msg_dict["encntr_dx_hist"])
#             for idx in range(len(msg_dict_encntr_dx_hist)):
#                 encntr_dx_hist_rows.append(msg_dict_encntr_dx_hist[idx])
            
#             encntr_er_complnt_hist_rows.append(eval(msg_dict["encntr_er_complnt_hist"]))
# #             adt_hist_rows.append(eval(msg_dict["adt_hist"]))

#             msg_dict_encntr_visit_rsn_hist = eval(msg_dict["encntr_visit_rsn_hist"])
#             for idx in range(len(msg_dict_encntr_visit_rsn_hist)):
#                 encntr_visit_rsn_hist_rows.append(msg_dict_encntr_visit_rsn_hist[idx])
            
#             receiver.complete_message(msg)
        
          
#         end_time = time.time()
#         print("Read {} msgs in Time : {} ".format(str(len(received_msgs)),end_time-start_time))    
            
#         print("INSERT STARTED.....")
#         print(datetime.now())
            
#         adt_hist_df = drop_na(to_timestamp_cols(spark.createDataFrame(adt_hist_rows,adt_hist_schema)\
#                           .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast(DecimalType(18,0)))\
#                           .withColumn("hsp_account_id",col("hsp_account_id").cast(DecimalType(18,0)))\
#                           .withColumn("row_insert_tsp", lit(datetime.now()))\
#                           .withColumn("row_updt_tsp", lit(datetime.now()))\
#                           .withColumn("update_user_id",col("insert_user_id")),timestamp_cols))\
#                           .select(adt_hist_columns) \
#                           .na.drop(subset=["msg_src","pat_enc_csn_id"]) 
  
#         write_to_synapse(adt_hist_df, 'epic_rltm.adt_hist_mult_cnsmr',mode='append') 
#         adt_hist_rows = []

#         encntr_dx_df =  drop_na(to_timestamp_cols(spark.createDataFrame(encntr_dx_hist_rows,encntr_dx_hist_schema)\
#                                     .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
#                                     .withColumn("row_insert_tsp", lit(datetime.now()))\
#                                     .withColumn("row_updt_tsp", lit(datetime.now()))\
#                                     .withColumn("update_user_id",col("insert_user_id")),timestamp_cols))\
#                                     .select(encntr_dx_hist_columns) \
#                                     .na.drop(subset=["msg_src","pat_enc_csn_id"])
              
#         write_to_synapse(encntr_dx_df, 'epic_rltm.encntr_dx_hist_mult_cnsmr',mode='append')
#         encntr_dx_hist_rows = []

#         encntr_er_df =  drop_na(to_timestamp_cols(spark.createDataFrame(encntr_er_complnt_hist_rows,encntr_er_complnt_hist_schema)\
#                                                               .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
#                                                               .withColumn("row_insert_tsp", lit(datetime.now()))\
#                                                               .withColumn("row_updt_tsp", lit(datetime.now()))\
#                                                               .withColumn("update_user_id",col("insert_user_id")),timestamp_cols))\
#                                                               .select(encntr_er_complnt_hist_columns) \
#                                                               .na.drop(subset=["msg_src","pat_enc_csn_id"])

#         write_to_synapse(encntr_er_df, 'epic_rltm.encntr_er_complnt_hist_mult_cnsmr',mode='append')
#         encntr_er_complnt_hist_rows = []

#         encntr_visit_rsn_df = drop_na(to_timestamp_cols(spark.createDataFrame(encntr_visit_rsn_hist_rows,encntr_visit_rsn_hist_schema)\
#                                                   .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
#                                                   .withColumn("row_insert_tsp", lit(datetime.now()))\
#                                                   .withColumn("row_updt_tsp", lit(datetime.now()))\
#                                                   .withColumn("update_user_id",col("insert_user_id")),timestamp_cols))\
#                                                   .select(encntr_visit_rsn_columns) \
#                                                   .na.drop(subset=["msg_src","pat_enc_csn_id"])
                  
#         write_to_synapse(encntr_visit_rsn_df, 'epic_rltm.encntr_visit_rsn_hist_mult_cnsmr',mode='append')
#         encntr_visit_rsn_hist_rows = []
            
#         print(datetime.now())
#         print("INSERT ENDED.......")


# COMMAND ----------

adt_hist_rows = []
encntr_dx_hist_rows = []
encntr_er_complnt_hist_rows = []
encntr_visit_rsn_hist_rows = []
staging_msgs = []
staging_table_name = "epic_rltm.adt_hist_mult_cnsmr_synapse_staging"
batch_size = 500
batch_id = 0

timestamp_cols = ["msg_tm", "birth_date","death_date","hosp_admsn_time","hosp_disch_time","adt_arrival_time"]

with servicebus_client_topic_receive:
    receiver = servicebus_client_topic_receive.get_subscription_receiver(topic_name=TOPIC_NAME, subscription_name=SUBSCRIPTION_3_NAME)
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
                  
              adt_hist_df = drop_na(to_timestamp_cols(spark.createDataFrame(adt_hist_rows,adt_hist_schema)\
                                .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast(DecimalType(18,0)))\
                                .withColumn("hsp_account_id",col("hsp_account_id").cast(DecimalType(18,0)))\
                                .withColumn("row_insert_tsp", lit(datetime.now()))\
                                .withColumn("row_updt_tsp", lit(datetime.now()))\
                                .withColumn("update_user_id",col("insert_user_id")),timestamp_cols))\
                                .select(adt_hist_columns) \
                                .na.drop(subset=["msg_src","pat_enc_csn_id"]) 
        
              write_to_synapse(adt_hist_df, 'epic_rltm.adt_hist_mult_cnsmr',mode='append') 
              adt_hist_rows = []
      
              encntr_dx_df =  drop_na(to_timestamp_cols(spark.createDataFrame(encntr_dx_hist_rows,encntr_dx_hist_schema)\
                                          .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
                                          .withColumn("row_insert_tsp", lit(datetime.now()))\
                                          .withColumn("row_updt_tsp", lit(datetime.now()))\
                                          .withColumn("update_user_id",col("insert_user_id")),timestamp_cols))\
                                          .select(encntr_dx_hist_columns) \
                                          .na.drop(subset=["msg_src","pat_enc_csn_id"])
                    
              write_to_synapse(encntr_dx_df, 'epic_rltm.encntr_dx_hist_mult_cnsmr',mode='append')
              encntr_dx_hist_rows = []
      
              encntr_er_df =  drop_na(to_timestamp_cols(spark.createDataFrame(encntr_er_complnt_hist_rows,encntr_er_complnt_hist_schema)\
                                                                    .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
                                                                    .withColumn("row_insert_tsp", lit(datetime.now()))\
                                                                    .withColumn("row_updt_tsp", lit(datetime.now()))\
                                                                    .withColumn("update_user_id",col("insert_user_id")),timestamp_cols))\
                                                                    .select(encntr_er_complnt_hist_columns) \
                                                                    .na.drop(subset=["msg_src","pat_enc_csn_id"])
      
              write_to_synapse(encntr_er_df, 'epic_rltm.encntr_er_complnt_hist_mult_cnsmr',mode='append')
              encntr_er_complnt_hist_rows = []
      
              encntr_visit_rsn_df = drop_na(to_timestamp_cols(spark.createDataFrame(encntr_visit_rsn_hist_rows,encntr_visit_rsn_hist_schema)\
                                                        .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast("decimal(18,0)"))\
                                                        .withColumn("row_insert_tsp", lit(datetime.now()))\
                                                        .withColumn("row_updt_tsp", lit(datetime.now()))\
                                                        .withColumn("update_user_id",col("insert_user_id")),timestamp_cols))\
                                                        .select(encntr_visit_rsn_columns) \
                                                        .na.drop(subset=["msg_src","pat_enc_csn_id"])
                        
              write_to_synapse(encntr_visit_rsn_df, 'epic_rltm.encntr_visit_rsn_hist_mult_cnsmr',mode='append')
              encntr_visit_rsn_hist_rows = []
                  
              print(datetime.now())
              print("INSERT ENDED.......")
              
              empty_staging(staging_table_name)
              print("Emptied Staging Layer...")
              staging_msgs = []
              
        except Exception as error:
          print("Retrying batch : {}".format(batch_id))
          batch_id = batch_id - 1
