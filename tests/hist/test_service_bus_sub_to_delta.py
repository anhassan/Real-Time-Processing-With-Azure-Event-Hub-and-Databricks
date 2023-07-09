# Databricks notebook source
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from hl7apy import parser 
from pyspark.sql.functions import to_timestamp,lit, col
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, LongType,DecimalType
from delta import DeltaTable
from datetime import datetime
import pandas as pd
from pandas.testing import assert_frame_equal


# COMMAND ----------

#%run /Shared/utils/env_var

# COMMAND ----------

#%run ../../app/epic_rltm_service_bus_sub1_to_delta

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
#         StructField("row_insert_tsp", StringType(), True),
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
            StructField("insert_user_id", StringType(), True),
        ]
    )


# COMMAND ----------

adt_hist_schema_sink = StructType(
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
        StructField("row_insert_tsp", StringType(), True),
        StructField("insert_user_id", StringType(), True),     
    ]
)

encntr_dx_hist_schema_sink =StructType(
        [
            StructField("msg_src", StringType(), True),
            StructField("msg_tm", StringType(), True),
            StructField("pat_enc_csn_id", LongType(), True),
            StructField("dx_icd_cd", StringType(), True),
            StructField("dx_name", StringType(), True),
            StructField("dx_code_type", StringType(), True),
            StructField("row_insert_tsp", StringType(), True),
            StructField("insert_user_id", StringType(), True),
            
        ]
    )

encntr_er_complnt_hist_schema_sink = StructType(
    [
        StructField("msg_src", StringType(), True),
        StructField("msg_tm", StringType(), True),
        StructField("pat_enc_csn_id", LongType(), True),
        StructField("er_complaint", StringType(), True),
        StructField("row_insert_tsp", StringType(), True),
        StructField("insert_user_id", StringType(), True),
        
    ]
)

encntr_visit_rsn_hist_schema_sink = StructType(
        [
            StructField("msg_src", StringType(), True),
            StructField("msg_tm", StringType(), True),
            StructField("pat_enc_csn_id", LongType(), True),
            StructField("encntr_rsn", StringType(), True),
            StructField("row_insert_tsp", StringType(), True),
            StructField("insert_user_id", StringType(), True),
        ]
    )


adt_hist_columns = ['msg_typ','msg_src','trigger_evnt','msg_tm','msg_nm','bod_id','pat_mrn_id','pat_enc_csn_id','birth_date','death_date','pat_class_abbr',
 'hosp_admsn_time','hosp_disch_time','department_abbr','loc_abbr','room_nm','bed_label','bed_status','sex_abbr','means_of_arrv_abbr',
  'acuity_level_abbr','ed_disposition_abbr','disch_disp_abbr','adt_arrival_time','hsp_account_id','accommodation_abbr','user_id',
  'row_insert_tsp','insert_user_id']

encntr_dx_hist_columns = ["msg_src","msg_tm","pat_enc_csn_id","dx_icd_cd","dx_name","dx_code_type","row_insert_tsp","insert_user_id"]
encntr_er_complnt_hist_columns = [
    "msg_src",
    "msg_tm",
    "pat_enc_csn_id",
    "er_complaint",
    "row_insert_tsp",
    "insert_user_id",
    
]

encntr_visit_rsn_columns=["msg_src","msg_tm","pat_enc_csn_id","encntr_rsn","row_insert_tsp","insert_user_id"]

# COMMAND ----------

# helper functions

msg = """{"adt_hist":"{'msg_typ': 'ADT', 'msg_src': 'STL', 'trigger_evnt': 'A03', 'msg_tm': '2022-06-02 16:07:42-05:00', 'msg_nm': 1101, 'bod_id': '563782752', 'pat_mrn_id': 'E1404972671', 'pat_enc_csn_id': 248762958, 'birth_date': '1950-08-20 00:00:00-05:00', 'death_date': None, 'pat_class_abbr': 'OBSERVATION', 'hosp_admsn_time': None, 'hosp_disch_time': '2022-06-02 16:07:00-05:00', 'department_abbr': 'SJMED', 'loc_abbr': 'SJM', 'room_nm': 'CHECKOUT', 'bed_label': 'CHECKOUT', 'bed_status': 'Ready', 'sex_abbr': 'M', 'means_of_arrv_abbr': 'Car', 'acuity_level_abbr': 'Emergent', 'ed_disposition_abbr': 'AMA', 'disch_disp_abbr': 'HOME', 'adt_arrival_time': None, 'hsp_account_id': 61001224159, 'accommodation_abbr': None, 'user_id': '39248', 'insert_user_id': 'smhatar1@mercy.net'}","encntr_dx_hist":"[{'msg_src': 'STL', 'msg_tm': '2022-06-02 16:07:42-05:00', 'pat_enc_csn_id': 248762958, 'dx_icd_cd': 'R00.1', 'dx_name': 'Bradycardia, unspecified', 'dx_code_type': 'ICD-10-CM', 'insert_user_id': 'smhatar1@mercy.net'}]","encntr_er_complnt_hist":"{'msg_src': None, 'msg_tm': None, 'pat_enc_csn_id': None, 'er_complaint': None, 'insert_user_id': 'smhatar1@mercy.net'}","encntr_visit_rsn_hist":"[{'msg_src': 'STL', 'msg_tm': '2022-06-02 16:07:42-05:00', 'pat_enc_csn_id': 248762958, 'encntr_rsn': 'Dizziness', 'insert_user_id': 'smhatar1@mercy.net'}]"}"""

def send_a_list_of_messages_to_topic(topic_conn_str, topic_name, payloads):        
    messages = [ServiceBusMessage(payload) for payload in payloads]     
    servicebus_client_topic_send = ServiceBusClient.from_connection_string(topic_conn_str, logging_enable=True)
          
    with servicebus_client_topic_send:
      sender = servicebus_client_topic_send.get_topic_sender(topic_name)
      with sender:
          sender.send_messages(messages)
   

# COMMAND ----------

#Code to test

def insert_into_adt_hist(adt_hist_rows, adt_hist_schema_src, adt_hist_table):
 
  spark.createDataFrame(adt_hist_rows, adt_hist_schema_src).withColumn("birth_date", to_timestamp("birth_date")) \
     .withColumn("death_date", to_timestamp("death_date")) \
     .withColumn("msg_tm", to_timestamp("msg_tm")) \
     .withColumn("hosp_admsn_time", to_timestamp("hosp_admsn_time")) \
     .withColumn("hosp_disch_time", to_timestamp("hosp_disch_time")) \
     .withColumn("adt_arrival_time", to_timestamp("adt_arrival_time")) \
     .withColumn("row_insert_tsp", lit(datetime.now())) \
     .withColumn("pat_enc_csn_id", col("pat_enc_csn_id").cast(DecimalType(18, 0))) \
     .withColumn("hsp_account_id", col("hsp_account_id").cast(DecimalType(18, 0))) \
     .select(adt_hist_columns) \
     .na.drop(subset=["msg_src","pat_enc_csn_id"]) \
     .write.format("delta").mode("append").insertInto(adt_hist_table)
  
def insert_into_encntr_dx_hist(encntr_dx_hist_rows, encntr_dx_hist_schema_src, encntr_dx_hist_table):

  spark.createDataFrame(encntr_dx_hist_rows, encntr_dx_hist_schema_src) \
    .withColumn("row_insert_tsp", lit(datetime.now())) \
    .withColumn("pat_enc_csn_id", col("pat_enc_csn_id").cast(DecimalType(18, 0))) \
    .select(encntr_dx_hist_columns) \
    .na.drop(subset=["msg_src","pat_enc_csn_id"]) \
    .write.format("delta").mode("append").insertInto(encntr_dx_hist_table) 
  
def insert_into_encntr_er_complnt_hist(encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src, encntr_er_complnt_hist_table):

  spark.createDataFrame(encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src) \
      .withColumn("msg_tm", to_timestamp("msg_tm")).withColumn("row_insert_tsp", lit(datetime.now())) \
      .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast(DecimalType(18,0))) \
      .select(encntr_er_complnt_hist_columns) \
      .na.drop(subset=["msg_src","pat_enc_csn_id"]) \
      .write.format("delta").mode("append").insertInto(encntr_er_complnt_hist_table)
  
def insert_into_encntr_visit_rsn_hist(encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src, encntr_visit_rsn_hist_table):

  spark.createDataFrame(encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src) \
      .withColumn("row_insert_tsp", lit(datetime.now())) \
      .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast(DecimalType(18,0))) \
      .select(encntr_visit_rsn_columns) \
      .na.drop(subset=["msg_src","pat_enc_csn_id"]) \
      .write.format("delta").mode("append").insertInto(encntr_visit_rsn_hist_table)
  
  
  
def sub_to_delta(service_bus_topic_receive_conn_str, service_bus_topic_name, sub_name, table_args_sink, table_args_src):
  
  servicebus_client_topic_receive = ServiceBusClient.from_connection_string(service_bus_topic_receive_conn_str, logging_enable=True)

  adt_hist_rows = []
  encntr_dx_hist_rows = []
  encntr_er_complnt_hist_rows = []
  encntr_visit_rsn_hist_rows = []
  batch_size = 2

  with servicebus_client_topic_receive:
      receiver = servicebus_client_topic_receive.get_subscription_receiver(topic_name=service_bus_topic_name, subscription_name=sub_name, max_wait_time=5)
      with receiver:
          for msg in receiver:
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
              if len(adt_hist_rows) == batch_size:
                
                  insert_into_adt_hist(
                    adt_hist_rows, adt_hist_schema_src=table_args_src["adt_hist"]["schema"], 
                    adt_hist_table=table_args_sink["adt_hist"]["table"]
                  )
                  adt_hist_rows = []
    
                  insert_into_encntr_dx_hist(
                    encntr_dx_hist_rows, encntr_dx_hist_schema_src=table_args_src["encntr_dx_hist"]["schema"], 
                    encntr_dx_hist_table=table_args_sink["encntr_dx_hist"]["table"]
                  )
                  encntr_dx_hist_rows = []
        
                  insert_into_encntr_er_complnt_hist(
                    encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src=table_args_src["encntr_er_complnt_hist"]["schema"], 
                    encntr_er_complnt_hist_table=table_args_sink["encntr_er_complnt_hist"]["table"]
                  )
                  encntr_er_complnt_hist_rows = []
            
                  insert_into_encntr_visit_rsn_hist(
                    encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src=table_args_src["encntr_visit_rsn_hist"]["schema"], 
                    encntr_visit_rsn_hist_table=table_args_sink["encntr_visit_rsn_hist"]["table"]
                  )
                  encntr_visit_rsn_hist_rows = []
                
table_args_src = {
  "adt_hist": {
    "schema": adt_hist_schema_src
  },
  "encntr_dx_hist": {
    "schema": encntr_dx_hist_schema_src
  },
  "encntr_er_complnt_hist": {
    "schema": encntr_er_complnt_hist_schema_src
  },
  "encntr_visit_rsn_hist": {
    "schema": encntr_visit_rsn_hist_schema_src
  }
}

sub_name="sub1"

service_bus_topic_receive_conn_str ="Endpoint=sb://sb-poc-hl7-001.servicebus.windows.net/;SharedAccessKeyName=top1_receive;SharedAccessKey=jTazOUqqrYpX0cUUjoyRdOwc4S8FIjy63uPnrwraUjI=;EntityPath=top1"

service_bus_topic_name="top1"

# SUB_1_NAME = "parsed_hl7_adt_to_delta_hist"
# sub_to_delta(service_bus_topic_receive_conn_str, service_bus_topic_name, sub_name, table_args_src)

# COMMAND ----------

def test_sub_to_delta(msg, table_args_sink, table_args_src):
  
  # sending msg to topic
  service_bus_topic_send_conn_str="Endpoint=sb://sb-poc-hl7-001.servicebus.windows.net/;SharedAccessKeyName=top1_send;SharedAccessKey=w18Jjyy2u+CgGa6Lwy/W5kILVNpx9/dA5m6KJ3eJypc=;EntityPath=top1"
  topic_name="top1"
  payloads=[msg, msg]
  send_a_list_of_messages_to_topic(
      service_bus_topic_send_conn_str, topic_name, payloads
  )
  
  # receiving from topic/sub and testing
  for arg in table_args_sink:
    table_args_sink[arg]["table"] += "_unit_test"
    hist_table_name = table_args_sink[arg]["table"]
    hist_table_loc = "{}/curated/epic_rltm/{}".format(os.getenv("adls_file_path"), hist_table_name)
    spark.sql("DROP TABLE IF EXISTS " + hist_table_name)

    spark.createDataFrame([],table_args_sink[arg]["schema"]).write.format("delta").mode("overwrite").save(hist_table_loc)
    spark.sql("CREATE TABLE IF NOT EXISTS " + hist_table_name + " USING DELTA LOCATION '" + hist_table_loc + "'")

       
  sub_to_delta(service_bus_topic_receive_conn_str, service_bus_topic_name, sub_name, table_args_sink, table_args_src)
  
  returned_adt_hist_sb_cnsmr_pd_df = spark.read.format("delta").load("{}/curated/epic_rltm/epic_rltm.adt_hist_sb_cnsmr_unit_test".format(os.getenv("adls_file_path"))).toPandas().drop('row_insert_tsp', axis=1)
  returned_encntr_dx_hist_sb_cnsmr_pd_df = spark.read.format("delta").load("{}/curated/epic_rltm/epic_rltm.encntr_dx_hist_sb_cnsmr_unit_test".format(os.getenv("adls_file_path"))).toPandas().drop('row_insert_tsp', axis=1)
  returned_encntr_er_complnt_hist_sb_pd_df = spark.read.format("delta").load("{}/curated/epic_rltm/epic_rltm.encntr_er_complnt_hist_sb_cnsmr_unit_test".format(os.getenv("adls_file_path"))).toPandas().drop('row_insert_tsp', axis=1)
  returned_encntr_visit_rsn_hist_sb_pd_df = spark.read.format("delta").load("{}/curated/epic_rltm/epic_rltm.encntr_visit_rsn_hist_sb_cnsmr_unit_test".format(os.getenv("adls_file_path"))).toPandas().drop('row_insert_tsp', axis=1)
  
  expected_adt_hist_pd_df = pd.DataFrame([['ADT', 'STL', 'A03', '2022-06-02 16:07:42', '1101', '563782752', 'E1404972671', 248762958, '1950-08-20 00:00:00', None, 'OBSERVATION', None, '2022-06-02 16:07:00', 'SJMED', 'SJM', 'CHECKOUT', 'CHECKOUT', 'Ready', 'M', 'Car', 'Emergent', 'AMA', 'HOME', None, 61001224159, None, '39248', 'smhatar1@mercy.net'],
           ['ADT', 'STL', 'A03', '2022-06-02 16:07:42', '1101', '563782752', 'E1404972671', 248762958, '1950-08-20 00:00:00', None, 'OBSERVATION', None, '2022-06-02 16:07:00', 'SJMED', 'SJM', 'CHECKOUT', 'CHECKOUT', 'Ready', 'M', 'Car', 'Emergent', 'AMA', 'HOME', None, 61001224159, None, '39248', 'smhatar1@mercy.net']], columns=[c for c in adt_hist_columns if c != "row_insert_tsp"])

  expected_encntr_dx_hist_pd_df = pd.DataFrame([['STL', '2022-06-02 16:07:42-05:00', 248762958, 'R00.1', 'Bradycardia, unspecified', 'ICD-10-CM', 'smhatar1@mercy.net'],
             ['STL', '2022-06-02 16:07:42-05:00', 248762958, 'R00.1', 'Bradycardia, unspecified', 'ICD-10-CM', 'smhatar1@mercy.net']], columns=[c for c in encntr_dx_hist_columns if c != "row_insert_tsp"])

  expected_encntr_er_complnt_hist_pd_df= pd.DataFrame([],columns=[c for c in encntr_er_complnt_hist_columns if c != "row_insert_tsp"])

  expected_encntr_visit_rsn_hist_pd_df =pd.DataFrame([['STL', '2022-06-02 16:07:42-05:00', 248762958, 'Dizziness', 'smhatar1@mercy.net'],
                                                     ['STL', '2022-06-02 16:07:42-05:00', 248762958, 'Dizziness', 'smhatar1@mercy.net']],columns=[c for c in encntr_visit_rsn_columns if c != "row_insert_tsp"])


  assert_frame_equal(expected_adt_hist_pd_df,returned_adt_hist_sb_cnsmr_pd_df)
  assert_frame_equal(expected_encntr_dx_hist_pd_df,returned_encntr_dx_hist_sb_cnsmr_pd_df)
  assert_frame_equal(expected_encntr_er_complnt_hist_pd_df,returned_encntr_er_complnt_hist_sb_pd_df)
  assert_frame_equal(expected_encntr_visit_rsn_hist_pd_df,returned_encntr_visit_rsn_hist_sb_pd_df)
  
  print("Test Case Passed Successfully")
  
table_args_sink = {
  "adt_hist": {
    "table": "epic_rltm.adt_hist_sb_cnsmr",
    "schema": adt_hist_schema_sink
  },
  "encntr_dx_hist": {
    "table": "epic_rltm.encntr_dx_hist_sb_cnsmr",
    "schema": encntr_dx_hist_schema_sink
  },
  "encntr_er_complnt_hist": {
    "table": "epic_rltm.encntr_er_complnt_hist_sb_cnsmr",
    "schema": encntr_er_complnt_hist_schema_sink
  },
  "encntr_visit_rsn_hist": {
    "table": "epic_rltm.encntr_visit_rsn_hist_sb_cnsmr",
    "schema": encntr_visit_rsn_hist_schema_sink
  }
}
test_sub_to_delta(msg, table_args_sink, table_args_src)