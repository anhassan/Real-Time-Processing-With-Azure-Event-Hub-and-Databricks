# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook consumes messages from Service Bus queue and sends the data  to Topic and the number of subscribers attatched to it. 
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 08/30/2022     AHASSAN2          Initial Creation


# COMMAND ----------

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from hl7apy import parser 
from pyspark.sql.functions import to_timestamp,lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# COMMAND ----------

# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

# MAGIC %run ../code/hl7_sb/adt_hist

# COMMAND ----------

# MAGIC %run ../code/hl7_sb/encntr_dx_hist

# COMMAND ----------

# MAGIC %run ../code/hl7_sb/encntr_er_complnt_hist

# COMMAND ----------

# MAGIC %run ../code/hl7_sb/encntr_visit_rsn_hist

# COMMAND ----------

# MAGIC %md
# MAGIC #### Current User

# COMMAND ----------

def get_current_user():
  query = 'select current_user' 
  current_user = spark.sql(query)
  return current_user.collect()[0][0]

insert_user_id = get_current_user()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Send the Message to Topic

# COMMAND ----------

def send_a_list_of_messages_to_topic(topic_conn_str, topic_name, payloads):        
    messages = [ServiceBusMessage(payload) for payload in payloads]     
    servicebus_client_topic_send = ServiceBusClient.from_connection_string(topic_conn_str, logging_enable=True)
          
    with servicebus_client_topic_send:
      sender = servicebus_client_topic_send.get_topic_sender(topic_name)
      with sender:
          sender.send_messages(messages)

# COMMAND ----------

def invoke_all(messages):
  
    df_messages = spark.createDataFrame(messages, StructType([StructField("msg", StringType()), StructField("msg_nm", IntegerType())])).withColumn("insert_user_id", lit(insert_user_id))
    
    df_hist = df_messages.withColumn("adt_hist", invoke_adt_hist_process("msg", "insert_user_id", "msg_nm")) \
      .withColumn("encntr_dx_hist",invoke_encntr_dx_process("msg","insert_user_id")) \
      .withColumn("encntr_er_complnt_hist",invoke_encntr_er_complaint_process("msg","insert_user_id")) \
      .withColumn("encntr_visit_rsn_hist",invoke_encntr_visit_rsn_process("msg","insert_user_id")) \
      .select("adt_hist","encntr_dx_hist", "encntr_er_complnt_hist","encntr_visit_rsn_hist").toJSON()
    
    return df_hist.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Receive from queue and send to the Topic

# COMMAND ----------

def queue_to_topic(queue_name, queue_conn_str, topic_name, topic_conn_str):
  
  servicebus_client_receive = ServiceBusClient.from_connection_string(queue_conn_str, logging_enable=True)
  messages = []
  batch_size = 500
  
  with servicebus_client_receive:
      receiver = servicebus_client_receive.get_queue_receiver(queue_name ) #max_wait_time=5
      with receiver:
          for msg in receiver:
              messages.append((str(msg), msg.sequence_number))
              receiver.complete_message(msg)
              if len(messages) == batch_size:
                  payloads = invoke_all(messages)
                  messages = []
                  send_a_list_of_messages_to_topic(topic_conn_str, topic_name, payloads)
            

# COMMAND ----------

queue_to_topic(service_bus_queue_name, service_bus_queue_receive_conn_str, service_bus_topic_name, service_bus_topic_send_conn_str)