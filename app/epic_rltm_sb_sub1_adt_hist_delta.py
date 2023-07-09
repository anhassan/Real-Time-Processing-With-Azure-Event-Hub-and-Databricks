# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook consumes messages from Service Bus Topic and writes the data to the ADT Delta Hist Tables in Databricks
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 05/26/2023     AHASSAN2          Added MERGE logic to handle duplication when stagging itable s not emptied and data is already in target table.

# COMMAND ----------

!pip install uamqp

# COMMAND ----------

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from pyspark.sql.functions import to_timestamp,lit,col
from pyspark.sql.types import *
from datetime import datetime
import time
import os

# COMMAND ----------

# MAGIC %run ../utils/utilities

# COMMAND ----------

# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

# MAGIC %run ../code/hist/adt_hist

# COMMAND ----------



# COMMAND ----------

import traceback

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
        StructField("birth_dt", StringType(), True),
        StructField("death_dt", StringType(), True),
        StructField("pat_class_abbr", StringType(), True),
        StructField("hosp_admsn_tm", StringType(), True),
        StructField("hosp_disch_tm", StringType(), True),
        StructField("dept_abbr", StringType(), True),
        StructField("loc_abbr", StringType(), True),
        StructField("room_nm", StringType(), True),
        StructField("bed_label", StringType(), True),
        StructField("bed_stat", StringType(), True),
        StructField("sex_abbr", StringType(), True),
        StructField("means_of_arrv_abbr", StringType(), True),
        StructField("acuity_level_abbr", StringType(), True),
        StructField("ed_dsptn_abbr", StringType(), True),
        StructField("disch_dsptn_abbr", StringType(), True),
        StructField("adt_arvl_tm", StringType(), True),
        StructField("hsp_acct_id", LongType(), True),  # Long
        StructField("accommodation_abbr", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("insert_user_id", StringType(), True), 
        StructField("msg_enqueued_tsp", StringType(), True),
        StructField("pregnancy_flg", StringType(), True),

    ]
)

encntr_dx_hist_schema_src =StructType(
        [
            StructField("msg_src", StringType(), True),
            StructField("msg_tm", StringType(), True),
            StructField("pat_enc_csn_id", LongType(), True),
            StructField("dx_icd_cd", StringType(), True),
            StructField("dx_nm", StringType(), True),
            StructField("dx_cd_typ", StringType(), True),
            StructField("insert_user_id", StringType(), True),
            StructField("msg_enqueued_tsp", StringType(), True)
            
        ]
    )

encntr_er_complnt_hist_schema_src = StructType(
    [
        StructField("msg_src", StringType(), True),
        StructField("msg_tm", StringType(), True),
        StructField("pat_enc_csn_id", LongType(), True),
        StructField("er_complnt", StringType(), True),
        StructField("insert_user_id", StringType(), True),
        StructField("msg_enqueued_tsp", StringType(), True)
        
    ]
)

encntr_visit_rsn_hist_schema_src = StructType(
        [
            StructField("msg_src", StringType(), True),
            StructField("msg_tm", StringType(), True),
            StructField("pat_enc_csn_id", LongType(), True),
            StructField("encntr_rsn", StringType(), True),
            StructField("insert_user_id", StringType(), True),
            StructField("msg_enqueued_tsp", StringType(), True)
        ]
    )

encntr_nte_hist_schema_src = StructType(
        [
            StructField("msg_src", StringType(), True),
            StructField("msg_tm", StringType(), True),
            StructField("pat_enc_csn_id", LongType(), True),
            StructField("nte_txt", StringType(), True),
            StructField("nte_typ", StringType(), True),
            StructField("insert_user_id", StringType(), True),
            StructField("msg_enqueued_tsp", StringType(), True)
        ]
    )

# COMMAND ----------

adt_hist_columns = ['msg_typ','msg_src','trigger_evnt','msg_tm','msg_nm','bod_id','pat_mrn_id','pat_enc_csn_id','birth_dt','death_dt','pat_class_abbr',
                    'hosp_admsn_tm','hosp_disch_tm','dept_abbr','loc_abbr','room_nm','bed_label','bed_stat','sex_abbr','means_of_arrv_abbr',
                    'acuity_level_abbr','ed_dsptn_abbr','disch_dsptn_abbr','adt_arvl_tm','hsp_acct_id','accommodation_abbr','user_id',
                    'row_insert_tsp','row_updt_tsp','insert_user_id','updt_user_id', 'msg_enqueued_tsp', 'pregnancy_flg']

encntr_dx_hist_columns = ["msg_src","msg_tm","pat_enc_csn_id","dx_icd_cd","dx_nm","dx_cd_typ","row_insert_tsp","row_updt_tsp","insert_user_id","updt_user_id","msg_enqueued_tsp"]
encntr_er_complnt_hist_columns = ["msg_src","msg_tm","pat_enc_csn_id","er_complnt","row_insert_tsp","row_updt_tsp","insert_user_id","updt_user_id","msg_enqueued_tsp"]

encntr_visit_rsn_columns=["msg_src","msg_tm","pat_enc_csn_id","encntr_rsn","row_insert_tsp","row_updt_tsp","insert_user_id","updt_user_id","msg_enqueued_tsp"]
encntr_nte_columns=["msg_src","msg_tm","pat_enc_csn_id","nte_txt","nte_typ","row_insert_tsp","row_updt_tsp","insert_user_id","updt_user_id","msg_enqueued_tsp"]

# COMMAND ----------

def sub_to_delta_with_staging(service_bus_topic_receive_conn_str, service_bus_topic_name, sub_name, table_args_src):
  
  servicebus_client_topic_receive = ServiceBusClient.from_connection_string(service_bus_topic_receive_conn_str, logging_enable=True, uamqp_transport=True)

  adt_hist_rows = []
  encntr_dx_hist_rows = []
  encntr_er_complnt_hist_rows = []
  encntr_visit_rsn_hist_rows = []
  encntr_nte_hist_rows = []
  staging_msgs = []
  staging_table_name = "epic_rltm_stg.adt_hist_stg"
  batch_size = 1000
  batch_id = 0
  
  with servicebus_client_topic_receive:
      receiver = servicebus_client_topic_receive.get_subscription_receiver(topic_name=service_bus_topic_name, subscription_name=sub_name)#, max_wait_time=5
      with receiver:
        while 1==1:
          start_time = time.time()

          try:
            staged_msgs = read_from_staging(staging_table_name)

            if staged_msgs:
                [adt_hist_rows,encntr_dx_hist_rows,encntr_er_complnt_hist_rows,encntr_visit_rsn_hist_rows, encntr_nte_hist_rows] = parse_table_data(staged_msgs,[], True, False)

                print("MERGE STARTED.....")
                print(datetime.now())
                  
                merge_into_adt_hist(
                  adt_hist_rows, adt_hist_schema_src=table_args_src["adt_hist"]["schema"], 
                  adt_hist_table=table_args_src["adt_hist"]["table"],
                  adt_hist_table_loc=table_args_src["adt_hist"]["loc"]
                )
                adt_hist_rows = []
    
                merge_into_encntr_dx_hist(
                  encntr_dx_hist_rows, encntr_dx_hist_schema_src=table_args_src["encntr_dx_hist"]["schema"], 
                  encntr_dx_hist_table=table_args_src["encntr_dx_hist"]["table"],
                  encntr_dx_hist_table_loc=table_args_src["encntr_dx_hist"]["loc"]
                )
                encntr_dx_hist_rows = []
        
                merge_into_encntr_er_complnt_hist(
                  encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src=table_args_src["encntr_er_complnt_hist"]["schema"], 
                  encntr_er_complnt_hist_table=table_args_src["encntr_er_complnt_hist"]["table"],
                  encntr_er_complnt_hist_table_loc=table_args_src["encntr_er_complnt_hist"]["loc"]
                )
                encntr_er_complnt_hist_rows = []
            
                merge_into_encntr_visit_rsn_hist(
                  encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src=table_args_src["encntr_visit_rsn_hist"]["schema"], 
                  encntr_visit_rsn_hist_table=table_args_src["encntr_visit_rsn_hist"]["table"],
                  encntr_visit_rsn_hist_table_loc=table_args_src["encntr_visit_rsn_hist"]["loc"]
                )
                encntr_visit_rsn_hist_rows = []
                
                merge_into_encntr_nte_hist(
                  encntr_nte_hist_rows, encntr_nte_hist_schema_src=table_args_src["encntr_nte_hist"]["schema"], 
                  encntr_nte_hist_table=table_args_src["encntr_nte_hist"]["table"],
                  encntr_nte_hist_table_loc=table_args_src["encntr_nte_hist"]["loc"]
                )
                encntr_nte_hist_rows = []

                print(datetime.now())
                print("MERGE ENDED.......")
              
                empty_staging(staging_table_name)
                print("Emptied Staging Layer...")
                staging_msgs = []

          except Exception as error:
            print(traceback.print_exc())
            print("Exception occured during MERGE")
            continue

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
              [adt_hist_rows,encntr_dx_hist_rows,encntr_er_complnt_hist_rows,encntr_visit_rsn_hist_rows, encntr_nte_hist_rows] = parse_table_data(staged_msgs,unacked_msgs, True, False)
            
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
                
              insert_into_encntr_nte_hist(
                encntr_nte_hist_rows, encntr_nte_hist_schema_src=table_args_src["encntr_nte_hist"]["schema"], 
                encntr_nte_hist_table=table_args_src["encntr_nte_hist"]["table"]
              )
              encntr_nte_hist_rows = []

              print(datetime.now())
              print("INSERT ENDED.......")
              
              empty_staging(staging_table_name)
              print("Emptied Staging Layer...")
              staging_msgs = []
          
          except Exception as error:
            print(traceback.print_exc())
            print("Retrying batch : {}".format(batch_id))
            batch_id = batch_id - 1
            
                
table_args_src = {
  "adt_hist": {
    "table": "epic_rltm.adt_hist",
    "schema": adt_hist_schema_src,
    "loc" : "{}/raw/rltm/epic/adt_hist".format(os.getenv("adls_file_path"))
  },
  "encntr_dx_hist": {
    "table": "epic_rltm.encntr_dx_hist",
    "schema": encntr_dx_hist_schema_src,
    "loc" : "{}/raw/rltm/epic/encntr_dx_hist".format(os.getenv("adls_file_path"))
  },
  "encntr_er_complnt_hist": {
    "table": "epic_rltm.encntr_er_complnt_hist",
    "schema": encntr_er_complnt_hist_schema_src,
    "loc" : "{}/raw/rltm/epic/encntr_er_complnt_hist".format(os.getenv("adls_file_path"))
  },
  "encntr_visit_rsn_hist": {
    "table": "epic_rltm.encntr_visit_rsn_hist",
    "schema": encntr_visit_rsn_hist_schema_src,
    "loc" :  "{}/raw/rltm/epic/encntr_visit_rsn_hist".format(os.getenv("adls_file_path"))
  },
  "encntr_nte_hist": {
    "table": "epic_rltm.encntr_nte_hist",
    "schema": encntr_nte_hist_schema_src,
    "loc" :   "{}/raw/rltm/epic/encntr_nte_hist".format(os.getenv("adls_file_path"))
  }
}


sub_to_delta_with_staging(service_bus_topic_receive_conn_str, service_bus_topic_name, SUB_1_NAME, table_args_src)

# COMMAND ----------

