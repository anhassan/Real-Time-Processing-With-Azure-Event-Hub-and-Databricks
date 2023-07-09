# Databricks notebook source
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from hl7apy import parser 
from pyspark.sql.functions import to_timestamp,lit
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../../code/hl7_sb/adt_hist

# COMMAND ----------

# MAGIC %run ../../code/hl7_sb/encntr_dx_hist

# COMMAND ----------

# MAGIC %run ../../code/hl7_sb/encntr_er_complnt_hist

# COMMAND ----------

# MAGIC %run ../../code/hl7_sb/encntr_visit_rsn_hist

# COMMAND ----------

# code to test

def get_current_user():
  query = 'select current_user' 
  current_user = spark.sql(query)
  return current_user.collect()[0][0]



def invoke_all(messages, insert_user_id):
   
    df_messages = spark.createDataFrame(messages, StructType([StructField("msg", StringType()), StructField("msg_nm", IntegerType())])).withColumn("insert_user_id", lit(insert_user_id))
    
    df_hist = df_messages.withColumn("adt_hist", invoke_adt_hist_process("msg", "insert_user_id", "msg_nm")) \
      .withColumn("encntr_dx_hist",invoke_encntr_dx_process("msg","insert_user_id")) \
      .withColumn("encntr_er_complnt_hist",invoke_encntr_er_complaint_process("msg","insert_user_id")) \
      .withColumn("encntr_visit_rsn_hist",invoke_encntr_visit_rsn_process("msg","insert_user_id")) \
      .select("adt_hist","encntr_dx_hist", "encntr_er_complnt_hist","encntr_visit_rsn_hist").toJSON()
    
    return df_hist.collect()
  
  
def send_a_list_of_messages_to_topic(topic_conn_str, topic_name, payloads):        
    messages = [ServiceBusMessage(payload) for payload in payloads]     
    servicebus_client_topic_send = ServiceBusClient.from_connection_string(topic_conn_str, logging_enable=True)
          
    with servicebus_client_topic_send:
      sender = servicebus_client_topic_send.get_topic_sender(topic_name)
      with sender:
          sender.send_messages(messages)
          
          
def queue_to_topic(queue_name, queue_conn_str, topic_name, topic_conn_str, insert_user_id):
  
  servicebus_client_receive = ServiceBusClient.from_connection_string(queue_conn_str, logging_enable=True)
  messages = []
  batch_size = 1
  
  with servicebus_client_receive:
      receiver = servicebus_client_receive.get_queue_receiver(queue_name, max_wait_time=5)
      with receiver:
          for msg in receiver:
              messages.append((str(msg), 1101))
              receiver.complete_message(msg)
              if len(messages) == batch_size:
                  payloads = invoke_all(messages, insert_user_id)
                  messages = []
                  send_a_list_of_messages_to_topic(topic_conn_str, topic_name, payloads)
            

# COMMAND ----------

msg="""MSH|^~\&|EPIC|SJM|||20220602160742|39248|ADT^A03^ADT_A03|563782752|P|2.5.1
EVN|A03|20220602160742||ADT_EVENT|39248^MARION^EDWIN^J.^^^^^SJMHC^^^^^SJM|20220602160700
PID|1|E1404972671^^^EPI^EPI|E1404972671^^^EPI^EPI||SANBORN^HAROLD^EUGENE^^^^D~SANBORN^HAROLD^EUGENE^^^^L||19500820|M|SANBORN^HAROLD^^|CAU|16 MINERS TRAIL^^MOUNT IDA^AR^71957^US^P^^MONTGOMERY|MONTGOMERY|(661)599-1401^P^7^^^661^5991401~(661)619-1517^P^1^^^661^6191517||ENGLISH|MARRIED|CHR||545-74-8405|||NON-HISPANIC||||||||N
ZPD||MYCH||PT DECLINED||CHI ST VINCENT COMMUNITIES^^60|N|||||||||||N||||||M
PD1|||CHI MCAULEY MEDICAL OFFICE BUILDING MAIN CAMPUS^^60507|12256013^BRIDGES^JAMES^SCOTT^^^^^SOMSER^^^^SOMSER
ROL|1||PP|12256013^BRIDGES^JAMES^SCOTT^^^^^SOMSER^^^^SOMSER|20210421||||GENERAL|INTERNAL|1 MERCY LANE^STE 506^HOT SPRINGS^AR^71913-6462^|(501)622-6500^^8^^^501^6226500~(501)622-6575^^4^^^501^6226575
CON|1|HIN Opt Out|||||||||100019||20210616101254||20220414235959
CON|2|HIN Opt Out|||||||||100019||20220506081836||20230505235959
NK1|1|SANBORN^SHERRY^^|Spouse||||EC1
NK1|2|||16 MINERS TRAIL^^MOUNT IDA^AR^71957^US^^^MONTGOMERY|(661)599-1401^^7^^^661^5991401||EMP||||||NOT EMPLOYED||||||||||||||||||||1003|Retired
PV1|1|OBSERVATION|SJMED^CHECKOUT^CHECKOUT^SJM^Ready^^^^^^|ER|||12252616^RUBY^BETHANY^JO^^^^^SOMSER^^^^SOMSER||14232858^VENGALA^SRINIVAS^^^^^^SOMSER^^^^SOMSER|MED||||Home/Self|||||248762958|CARE||||||||||||||||HOME|HOME|||||||20220602131400|202206021607||846|||61001224159
PV2||||||||||||Hospital Encounter|||||||||2|N||||||||||N||||||Car||||||||||||20220602121600||20220602131400
ZPV||||||||||||20220602153300|20220602131400||AMA||||||||||||||||||||||||(501)622-1043^^^^^501^6221043
ROL|2||Consulting|14232858^VENGALA^SRINIVAS^^^^^^SOMSER^^^^SOMSER|20220602140850||||||200 HEART CENTER LANE^^HOT SPRINGS^AR^71913-6351^US^^^GARLAND|(501)625-8400^^8^^^501^6258400~(501)625-8446^^4^^^501^6258446
ROL|3||HOSPITALIST|12244878^HERRING^KATIE^MOAK^^^^^SOMSER^^^^SOMSER|20220602151939||||||1 MERCY LN^SUITE 404^HOT SPRINGS^AR^71913-6457^|(501)609-2222^^8^^^501^6092222~(501)321-9689^^4^^^501^3219689
ROL|4||RN|12279007^SEAGREN^MARIA^^^^^^SOMSER^^^^SOMSER|20220602134011
OBX|1|CWE|8661-1^CHIEF COMPLAINT:FIND:PT:PATIENT:NOM:REPORTED^LN|1|R00.1^Bradycardia, unspecified^ICD-10-CM||||||F
OBX|2|TX|8661-1^CHIEF COMPLAINT:FIND:PT:PATIENT:NOM:REPORTED:18100^LN|2|^^^^^^^^Dizziness||||||F
OBX|3|CWE|SS003^FACILITY / VISIT TYPE^LN|3|225XN1300X^PRIMARY CARE^NUCC||||||F
OBX|4|CWE|11283-9^ACUITY ASSESSMENT AT FIRST ENCOUNTER^LN|4|2^Emergent^HL70432||||||F
OBX|5|NM|11289-6^BODY TEMPERATURE:TEMP:ENCTRFRST:PATIENT:QN^LN|5|97.3|degF^Fahrenheit^UCUM|||||F|||20220602122400
OBX|6|NM|21612-7^AGE TIME PATIENT REPORTED^LN|6|71|a^YEAR^UCUM|||||F|||20220602160742
OBX|7|NM|59408-5^OXYGEN SATURATION:MFR:PR:BLDA:QN:PULSE OXIMETRY^LN|7|97|%{Oxygen}^PercentOxygen [Volume Fraction Units]^UCUM|||||F|||20220602122400
OBX|8|TX|54094-8^TRIAGE NOTE:FIND:PT:EMERGENCY DEPARTMENT:DOC^LN|8|Pt to ED23||||||F|||20220602160715
OBX|9|NM|6701-1^READMISSION RISK SCORE^LN|9|7||||||F|||20220602160700
AL1|1|Drug Class|12082233^NO KNOWN ALLERGIES^SOMELG
DG1|1|ICD-10-CM|R00.1^Bradycardia, unspecified^ICD-10-CM|Bradycardia, unspecified||18400;EPT
GT1|1|104865489|SANBORN^HAROLD^EUGENE^||16 MINERS TRAIL^^MOUNT IDA^AR^71957^US^^^MONTGOMERY|(661)599-1401^P^7^^^661^5991401~(661)619-1517^P^1^^^661^6191517||19500820|M|P/F|SLF|545-74-8405||||NOT EMPLOYED|16 MINERS TRAIL^^MOUNT IDA^AR^71957^US|(661)599-1401^^^^^661^5991401||Retired
IN1|1|4881^MEDICARE PART A AND B|924|MEDICARE|||||||||||5|SANBORN^HAROLD^EUGENE^|Self|19500820|16 MINERS TRAIL^^MOUNT IDA^AR^71957^US^^^MONTGOMERY|||1|||YES||||||||||6533224|7J12YW4XM00||||||Retired|M|16 MINERS TRAIL^^MOUNT IDA^AR^71957^US|||BOTH||6533224
IN2||545-74-8405|||Payer Plan||||||||||||||||||||||||||||||||||||||||||||||||||||||||7J12YW4XM00||(661)599-1401^^^^^661^5991401|(661)599-1401^^^^^661^5991401||||||NOT EMPLOYED
IN3|1||||||||||^
ZIN||||||||||Self||16819697|104865489|1||Other
IN1|2|9912^MERCY BLUE ACCESS CHOICE PPO|2609|ANTHEM|P.O. BOX 105187^^ATLANTA^GA^30348-5187^|||BS030X|||||||10|SANBORN^HAROLD^EUGENE^|Self|19500820|16 MINERS TRAIL^^MOUNT IDA^AR^71957^US^^^MONTGOMERY|||5|||YES||||||||||6699586|XDD683A50408||||||Retired|M|16 MINERS TRAIL^^MOUNT IDA^AR^71957^US|||BOTH||6699586
IN2||545-74-8405|||Payer Plan||||||||||||||||||||||||||||||||||||||||||||||||||||||||XDD683A50408||(661)599-1401^^^^^661^5991401|(661)599-1401^^^^^661^5991401||||||NOT EMPLOYED
IN3|1||||||||||^
ZIN||||||||||||16819697|104865489|1||Other"""

# COMMAND ----------

messages =[(msg, 950)]

# COMMAND ----------

def test_queue_to_topic(msg):
  
    servicebus_client = ServiceBusClient.from_connection_string(
      conn_str="Endpoint=sb://sb-poc-hl7-001.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=Wfpq0jU5kAUZnMX+X6O37BPMvGsbYq5ibQtoDUxUBmk=;EntityPath=adt_from_tibco",
QUEUE_NAME="adt_from_tibco", 
      logging_enable=True
    )
    with servicebus_client:
        sender = servicebus_client.get_queue_sender(queue_name="adt_from_tibco")
        with sender:
            messages = [ServiceBusMessage(msg) for _ in range(1)]
            sender.send_messages(messages)
            
    insert_user_id= "ahassan2@mercy.net"
    queue_name="adt_from_tibco"
    queue_conn_str="Endpoint=sb://sb-poc-hl7-001.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=MhSBNFfxIiFjpxfL2tGAaVbjkZtRj9f7FljbrsMT+9c=;EntityPath=adt_from_tibco"
    topic_name="top1"
    topic_conn_str="Endpoint=sb://sb-poc-hl7-001.servicebus.windows.net/;SharedAccessKeyName=top1_send;SharedAccessKey=WF572MQnJ2HFcUMobbGoxZ5PQUBHUNHJvt1cVnGJD/A=;EntityPath=top1"
    queue_to_topic(queue_name, queue_conn_str, topic_name, topic_conn_str, insert_user_id)
    
    servicebus_client_topic_receive = ServiceBusClient.from_connection_string("Endpoint=sb://sb-poc-hl7-001.servicebus.windows.net/;SharedAccessKeyName=top1_receive;SharedAccessKey=jTazOUqqrYpX0cUUjoyRdOwc4S8FIjy63uPnrwraUjI=;EntityPath=top1", logging_enable=True)
    returned = []
    with servicebus_client_topic_receive:
        receiver = servicebus_client_topic_receive.get_subscription_receiver(topic_name="top1", subscription_name="sub1", max_wait_time=5)
        with receiver:
            for msg in receiver:
                returned.append(str(msg))  
                
    expected = ["""{"adt_hist":"{'msg_typ': 'ADT', 'msg_src': 'STL', 'trigger_evnt': 'A03', 'msg_tm': '2022-06-02 16:07:42-05:00', 'msg_nm': 1101, 'bod_id': '563782752', 'pat_mrn_id': 'E1404972671', 'pat_enc_csn_id': 248762958, 'birth_date': '1950-08-20 00:00:00-05:00', 'death_date': None, 'pat_class_abbr': 'OBSERVATION', 'hosp_admsn_time': None, 'hosp_disch_time': '2022-06-02 16:07:00-05:00', 'department_abbr': 'SJMED', 'loc_abbr': 'SJM', 'room_nm': 'CHECKOUT', 'bed_label': 'CHECKOUT', 'bed_status': 'Ready', 'sex_abbr': 'M', 'means_of_arrv_abbr': 'Car', 'acuity_level_abbr': 'Emergent', 'ed_disposition_abbr': 'AMA', 'disch_disp_abbr': 'HOME', 'adt_arrival_time': None, 'hsp_account_id': 61001224159, 'accommodation_abbr': None, 'user_id': '39248', 'insert_user_id': 'smhatar1@mercy.net'}","encntr_dx_hist":"[{'msg_src': 'STL', 'msg_tm': '2022-06-02 16:07:42-05:00', 'pat_enc_csn_id': 248762958, 'dx_icd_cd': 'R00.1', 'dx_name': 'Bradycardia, unspecified', 'dx_code_type': 'ICD-10-CM', 'insert_user_id': 'smhatar1@mercy.net'}]","encntr_er_complnt_hist":"{'msg_src': None, 'msg_tm': None, 'pat_enc_csn_id': None, 'er_complaint': None, 'insert_user_id': 'smhatar1@mercy.net'}","encntr_visit_rsn_hist":"[{'msg_src': 'STL', 'msg_tm': '2022-06-02 16:07:42-05:00', 'pat_enc_csn_id': 248762958, 'encntr_rsn': 'Dizziness', 'insert_user_id': 'smhatar1@mercy.net'}]"}"""]
    
    if returned == expected:
       return "Test Case passed successfully"
    else:
       return "Test Case Failed"
  
test_queue_to_topic(msg)

# COMMAND ----------

def test_send_a_list_of_messages_to_topic():
  
    service_bus_topic_send_conn_str="Endpoint=sb://sb-poc-hl7-001.servicebus.windows.net/;SharedAccessKeyName=top1_send;SharedAccessKey=w18Jjyy2u+CgGa6Lwy/W5kILVNpx9/dA5m6KJ3eJypc=;EntityPath=top1"
    topic_name="top1"
    
    payloads=["test_msg_1", "test_msg_2"]
  
    send_a_list_of_messages_to_topic(
        service_bus_topic_send_conn_str, topic_name, payloads
    )
   
    servicebus_client_topic_receive = ServiceBusClient.from_connection_string(
        conn_str="Endpoint=sb://sb-poc-hl7-001.servicebus.windows.net/;SharedAccessKeyName=top1_receive;SharedAccessKey=YK+k4ZcV2R1o12u8Up0G2PmwNsvMNvkavTE2S1pi2hU=;EntityPath=top1",
        logging_enable=True,
    )
   
    returned = []
    with servicebus_client_topic_receive:
        receiver = servicebus_client_topic_receive.get_subscription_receiver(
            topic_name="top1", subscription_name="sub1",max_wait_time=5
        )
        with receiver:
            for msg in receiver:
                returned.append(str(msg))
                receiver.complete_message(msg)
    
    if returned == payloads:
       return "Test Case passed successfully"
    else:
       return "Test Case Failed"
    

test_send_a_list_of_messages_to_topic()

# COMMAND ----------

def test_get_current_user():
  expected = "smhatar1@mercy.net"
  returned = get_current_user()
  if returned == expected:
      return "Test Case passed successfully"
  else:
      return "Test Case Failed"
  
test_get_current_user()

# COMMAND ----------

def test_invoke_all():
  insert_user_id = "smhatar1@mercy.net"
  returned = invoke_all(messages, insert_user_id)
  expected = ["""{"adt_hist":"{'msg_typ': 'ADT', 'msg_src': 'STL', 'trigger_evnt': 'A03', 'msg_tm': '2022-06-02 16:07:42-05:00', 'msg_nm': 950, 'bod_id': '563782752', 'pat_mrn_id': 'E1404972671', 'pat_enc_csn_id': 248762958, 'birth_date': '1950-08-20 00:00:00-05:00', 'death_date': None, 'pat_class_abbr': 'OBSERVATION', 'hosp_admsn_time': None, 'hosp_disch_time': '2022-06-02 16:07:00-05:00', 'department_abbr': 'SJMED', 'loc_abbr': 'SJM', 'room_nm': 'CHECKOUT', 'bed_label': 'CHECKOUT', 'bed_status': 'Ready', 'sex_abbr': 'M', 'means_of_arrv_abbr': 'Car', 'acuity_level_abbr': 'Emergent', 'ed_disposition_abbr': 'AMA', 'disch_disp_abbr': 'HOME', 'adt_arrival_time': None, 'hsp_account_id': 61001224159, 'accommodation_abbr': None, 'user_id': '39248', 'insert_user_id': 'smhatar1@mercy.net'}","encntr_dx_hist":"[{'msg_src': 'STL', 'msg_tm': '2022-06-02 16:07:42-05:00', 'pat_enc_csn_id': 248762958, 'dx_icd_cd': 'R00.1', 'dx_name': 'Bradycardia, unspecified', 'dx_code_type': 'ICD-10-CM', 'insert_user_id': 'smhatar1@mercy.net'}]","encntr_er_complnt_hist":"{'msg_src': None, 'msg_tm': None, 'pat_enc_csn_id': None, 'er_complaint': None, 'insert_user_id': 'smhatar1@mercy.net'}","encntr_visit_rsn_hist":"[{'msg_src': 'STL', 'msg_tm': '2022-06-02 16:07:42-05:00', 'pat_enc_csn_id': 248762958, 'encntr_rsn': 'Dizziness', 'insert_user_id': 'smhatar1@mercy.net'}]"}"""]
  if returned == expected:
     return "Test Case passed successfully"
  else:
     return "Test Case Failed"
  
test_invoke_all()