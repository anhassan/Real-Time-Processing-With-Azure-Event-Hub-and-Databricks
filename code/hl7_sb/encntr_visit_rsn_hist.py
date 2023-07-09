# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType
from datetime import datetime
from pytz import timezone
import dateutil.parser

# COMMAND ----------

@udf
def invoke_encntr_visit_rsn_process(msg,insert_user_id,msg_enqueued_tsp):
  
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
#       return [num_cols*[None]+ [insert_user_id]]
      return str({"msg_src":None,
              "msg_tm": None,
              "pat_enc_csn_id":None,
              "encntr_rsn":None,
              "insert_user_id":insert_user_id,
              "msg_enqueued_tsp": None
              })
#         return None

#  "row_insert_tsp":None,

  try:
      msg_enqueued_tsp = str(msg_enqueued_tsp.astimezone(pytz.timezone('US/Central')))
  except:
      msg_enqueued_tsp = None
  
  observation_base_tag = "ns0:MessagePayload.ns0:AdministerPatient.VisitGroup.ns0:OBX.ObservationResult"
  identifier_tag = "OBX.3.ObservationIdentifier.ns0:OI.ObservationIdentifier.OI.1.Identifier"
  text_tag = "OBX.3.ObservationIdentifier.ns0:OI.ObservationIdentifier.OI.2.Text"
  visit_reason_tag = "OBX.5.ObservationValue_XAD.ns0:XAD.ExtendedAddress.XAD.9.CountyparishCode"
  visit_reason = []
  
  for obr in jsnmsg.OBX:

      try:
          identifier_tag = obr.OBX_3.CE_1.value if obr.OBX_3.CE_1.value else None 
      except:
          identifier_tag = None
      try:
          text_tag = obr.OBX_3.CE_2.value if obr.OBX_3.CE_2.value else None 
      except:
          text_tag = None 

      try:
          if identifier_tag == "8661-1" and text_tag == "CHIEF COMPLAINT:FIND:PT:PATIENT:NOM:REPORTED:18100":
              try:
                  reason = obr.OBX_5.VARIES_9.value if obr.OBX_5.VARIES_9.value else None 
                  reason_filtered = "" if reason is None else reason
                  visit_reason.append(reason_filtered)

              except:
                  visit_reason.append("")
      except:
            pass
      
      

  encntr_visit_rsn = [{"msg_src":msg_src,
                      "msg_tm": msg_time,
                      "pat_enc_csn_id":int(csn),
                      "encntr_rsn":reason,
#                       "row_insert_tsp":str(datetime.now()),
                      "insert_user_id":insert_user_id,
                      "msg_enqueued_tsp": msg_enqueued_tsp
                     } for reason in visit_reason if len(reason) > 0]
#   enctr_visit_rsn = [num_cols*[None]+[insert_user_id]] 
#   encntr_visit_rsn = []
  encntr_visit_rsn = [{"msg_src":None,
                      "msg_tm": None,
                      "pat_enc_csn_id":None,
                      "encntr_rsn":None,
#                       "row_insert_tsp":None,
                      "insert_user_id":insert_user_id,
                      "msg_enqueued_tsp": None
                      }] if len(encntr_visit_rsn) == 0 else encntr_visit_rsn
  
  return str(encntr_visit_rsn)
#   return None if len(encntr_visit_rsn) == 0 else str(encntr_visit_rsn)

# COMMAND ----------

encntr_visit_rsn_columns=["msg_src","msg_tm","pat_enc_csn_id","encntr_rsn","row_insert_tsp","insert_user_id","msg_enqueued_tsp"]