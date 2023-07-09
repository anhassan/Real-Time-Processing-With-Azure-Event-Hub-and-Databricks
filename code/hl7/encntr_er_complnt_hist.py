# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType
from datetime import datetime
from pytz import timezone
import dateutil.parser

# COMMAND ----------

def invoke_encntr_er_complaint_process(row):
  
  row_dict = row.asDict()
  msg = row_dict['body']
  insert_user_id = row_dict['insert_user_id']
  
  try:
    jsnmsg = parser.parse_message(msg.replace('\n','\r'), find_groups = False, validation_level=2)
  except:
    raise Exception("Invalid message")
  num_cols = 5
  
  try:
        msg_src = "STL"
        csn = int(jsnmsg.PV1.PV1_19.value) if jsnmsg.PV1.PV1_19.value else None
        msgtime = jsnmsg.MSH.MSH_7.value if jsnmsg.MSH.MSH_7.value else None
        msg_time = pytz.timezone('US/Central').localize(dateutil.parser.parse(msgtime))
  except:
        return num_cols*[None] + [insert_user_id]
    
  diagnosis_type_tag = "ns0:MessagePayload.ns0:AdministerPatient.ns0:DG1.Diagnosis.DG1.6.DiagnosisType"
  diagnosis_description_tag ="ns0:MessagePayload.ns0:AdministerPatient.ns0:DG1.Diagnosis.DG1.4.DiagnosisDescription"

  try:
      diagnosis_type_tag = jsnmsg.DG1.DG1_6.value if jsnmsg.DG1.DG1_6.value else None
  except:
      diagnosis_type_tag = None

  try:
      diagnosis_description_tag = jsnmsg.DG1.DG1_4.value if jsnmsg.DG1.DG1_4.value else None
  except:
      diagnosis_description_tag = None

  try:
      er_complaint = diagnosis_description_tag if diagnosis_type_tag == "10800;EPT" else [] 

      enctr_er_complnt = [msg_src,
                          msg_time,
                          int(csn),
                          er_complaint,
                          datetime.now(),insert_user_id
                         ] if len(er_complaint) > 0 else []
  except:
      enctr_er_complnt = []

  enctr_er_complnt = num_cols*[None]+[insert_user_id] if len(enctr_er_complnt) == 0 else enctr_er_complnt
  return enctr_er_complnt

# COMMAND ----------

enctr_er_complnt_hist_schema = StructType(
    [
        StructField("msg_src", StringType(), True),
        StructField("msg_tm", TimestampType(), True),
        StructField("pat_enc_csn_id", LongType(), True),
        StructField("er_complnt", StringType(), True),
        StructField("row_insert_tsp", TimestampType(), True),
        StructField("insert_user_id", StringType(), True),
        
    ]
)

# COMMAND ----------

enctr_er_complnt_hist_columns = [
    "msg_src",
    "msg_tm",
    "pat_enc_csn_id",
    "er_complnt",
    "row_insert_tsp",
    "insert_user_id",
    
]