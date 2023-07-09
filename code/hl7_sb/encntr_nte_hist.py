# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType
from datetime import datetime
from pytz import timezone
import dateutil.parser

# COMMAND ----------

@udf
def invoke_encntr_nte_process(msg,insert_user_id,msg_enqueued_tsp):
  
  try:
    jsnmsg = parser.parse_message(msg.replace('\n','\r'), find_groups = False, validation_level=2)
  except:
    raise Exception("Invalid message")

  try:
      msg_src = "STL"
      csn = int(jsnmsg.PV1.PV1_19.value) if jsnmsg.PV1.PV1_19.value else None
      msgtime = jsnmsg.MSH.MSH_7.value if jsnmsg.MSH.MSH_7.value else None
      msg_time = str(pytz.timezone('US/Central').localize(dateutil.parser.parse(msgtime)))
      
  except:
      return str({
        "msg_src":None,
        "msg_tm": None,
        "pat_enc_csn_id":None,
        "nte_txt":None,
        "nte_typ":None,
        "insert_user_id":insert_user_id,
        "msg_enqueued_tsp": None
      })
  
  try:
      msg_enqueued_tsp = str(msg_enqueued_tsp.astimezone(pytz.timezone('US/Central')))
  except:
      msg_enqueued_tsp = None

  nte_txt = ""
  nte_typ = 'ED'

  for obr in jsnmsg.OBX:
      try:
          if obr.obx_3.value == '54094-8^TRIAGE NOTE:FIND:PT:EMERGENCY DEPARTMENT:DOC^LN':
              nte_txt = obr.obx_5.value
      except:
          pass
      
  encntr_nte = {
    "msg_src":msg_src,
    "msg_tm": msg_time,
    "pat_enc_csn_id":int(csn),
    "nte_txt":nte_txt,
    "nte_typ":nte_typ,
    "insert_user_id":insert_user_id,
    "msg_enqueued_tsp": msg_enqueued_tsp
  }
  
  encntr_nte = {
    "msg_src":None,
    "msg_tm": None,
    "pat_enc_csn_id":None,
    "nte_txt":None,
    "nte_typ":None,
    "insert_user_id":insert_user_id,
    "msg_enqueued_tsp": None
  } if len(nte_txt) == 0 else encntr_nte
  
  return str(encntr_nte)