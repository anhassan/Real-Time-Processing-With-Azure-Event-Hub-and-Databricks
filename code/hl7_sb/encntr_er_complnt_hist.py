# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType
from datetime import datetime
from pytz import timezone
import dateutil.parser

# COMMAND ----------

@udf
def invoke_encntr_er_complaint_process(msg,insert_user_id,msg_enqueued_tsp):
  
  try:
    jsnmsg = parser.parse_message(msg.replace('\n','\r'), find_groups = False, validation_level=2)
  except:
    raise Exception("Invalid message")
  num_cols = 5
  
  try:
        msg_src = "STL"
        csn = int(jsnmsg.PV1.PV1_19.value) if jsnmsg.PV1.PV1_19.value else None
        msgtime = jsnmsg.MSH.MSH_7.value if jsnmsg.MSH.MSH_7.value else None
        msg_time = str(pytz.timezone('US/Central').localize(dateutil.parser.parse(msgtime)))
  except:
#         return num_cols*[None] + [insert_user_id]
        return str({"msg_src":None,
                "msg_tm":None,
                "pat_enc_csn_id":None,
                "er_complnt":None,
                "insert_user_id":insert_user_id,
                "msg_enqueued_tsp": None
                })
#           return str({})
  #                 "row_insert_tsp":None,
  
  try:
      msg_enqueued_tsp = str(msg_enqueued_tsp.astimezone(pytz.timezone('US/Central')))
  except:
      msg_enqueued_tsp = None

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
      er_complaint = diagnosis_description_tag if diagnosis_type_tag == "10800;EPT" or diagnosis_type_tag == 'E' else [] 

      encntr_er_complnt = {"msg_src":msg_src,
                          "msg_tm":msg_time,
                          "pat_enc_csn_id":int(csn),
                          "er_complnt":er_complaint,
#                         "row_insert_tsp":str(datetime.now()),
                          "insert_user_id":insert_user_id,
                          "msg_enqueued_tsp": msg_enqueued_tsp
                          } if len(er_complaint) > 0 else []
  except:
      encntr_er_complnt = []

#   enctr_er_complnt = num_cols*[None]+[insert_user_id] if len(enctr_er_complnt) == 0 else enctr_er_complnt
  encntr_er_complnt = {"msg_src":None,
                          "msg_tm":None,
                          "pat_enc_csn_id":None,
                          "er_complnt":None,
#                           "row_insert_tsp":None,
                          "insert_user_id":insert_user_id,
                          "msg_enqueued_tsp": None
                          } if len(encntr_er_complnt) == 0 else encntr_er_complnt
#   return None if len(encntr_er_complnt) == 0 else str(encntr_er_complnt)
  return str(encntr_er_complnt)

# COMMAND ----------

encntr_er_complnt_hist_columns = [
    "msg_src",
    "msg_tm",
    "pat_enc_csn_id",
    "er_complnt",
    "row_insert_tsp",
    "insert_user_id",
    "msg_enqueued_tsp"
    
]