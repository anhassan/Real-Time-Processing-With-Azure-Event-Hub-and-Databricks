# Databricks notebook source
from hl7apy import parser
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

med_admn_schema = StructType([
  StructField("msg_tm",StringType(),True),
  StructField("msg_typ",StringType(),True),
  StructField("pat_mrn_id",StringType(),True),
  StructField("pat_enc_csn_id",StringType(),True),
  StructField("ord_id",StringType(),True),
  StructField("ord_stat",StringType(),True),
  StructField("ord_admn",StringType(),True),
  StructField("ord_qnt",StringType(),True),
  StructField("ord_strt_tm",StringType(),True),
  StructField("ord_end_tm",StringType(),True),
  StructField("typ",StringType(),True),
  StructField("ord_crtr",StringType(),True),
  StructField("ord_prvdr_id",StringType(),True),
  StructField("ord_typ",StringType(),True),
  StructField("admn_tm",StringType(),True),
  StructField("med_id",StringType(),True),
  StructField("med_nm",StringType(),True),
  StructField("med_brnd_nm",StringType(),True),
  StructField("med_disp_nm",StringType(),True),
  StructField("dose",StringType(),True),
  StructField("units",StringType(),True),
  StructField("admn_prvdr",StringType(),True),
  StructField("admn_dept",StringType(),True),
  StructField("mar_actn",StringType(),True),
  StructField("med_route",StringType(),True)
])

# COMMAND ----------

@udf(returnType=med_admn_schema)
def parse_message(msg_val):

  try:
      parsed_msg = parser.parse_message(msg_val.replace("\n","\r"),find_groups=False,validation_level=2)
  except:
      raise Exception("Invalid Message")
  
  try:
      msg_tm = parsed_msg.msh.MSH_7.value
  except:
      msg_tm = None
  
  msg_typ = "RAS"

  try:
      pat_mrn_id = parsed_msg.pid.PID_3.PID_3_1.value
  except:
      pat_mrn_id = None
  
  try:
    pat_enc_csn_id = parsed_msg.PV1.PV1_19.value 
  except:
      pat_enc_csn_id = None

  try:
      ord_id = parsed_msg.ORC.ORC_2.ORC_2_1.value 
  except:
      ord_id = None

  try:
      ord_stat = parsed_msg.ORC.ORC_1.value
  except:
      ord_stat = None

  try:
      ord_admn = parsed_msg.ORC.ORC_9.value 
  except:
      ord_admn = None

  try:
      ord_qnt = parsed_msg.ORC.ORC_7.ORC_7_2.value
  except:
      ord_qnt = None

  try:
      ord_strt_tm = parsed_msg.ORC.ORC_7.ORC_7_4.value
  except:
      ord_strt_tm = None
    
  try:
      ord_end_tm = parsed_msg.ORC.ORC_7.ORC_7_5.value
  except:
      ord_end_tm = None

  try:
      typ = parsed_msg.ORC.ORC_7.ORC_7_6.value
  except:
      typ = None

  try:
      ord_crtr = parsed_msg.ORC.ORC_10.ORC_10_1.value
  except:
      ord_crtr = None

  try:
      ord_prvdr_id = parsed_msg.ORC.ORC_12.ORC_12_1.value
  except:
      ord_prvdr_id = None

  try:
      ord_typ = parsed_msg.ORC.ORC_29.value
  except:
      ord_typ = None

  try:
      admn_tm = parsed_msg.RXA.RXA_3.value
  except:
      admn_tm = None

  try:
      med_id = parsed_msg.RXA.RXA_5.RXA_5_1.value
  except:
      med_id = None

  try:
      med_nm = parsed_msg.RXA.RXA_5.RXA_5_2.value
  except:
      med_nm = None

  try:
      med_brnd_nm = parsed_msg.RXA.RXA_5.RXA_5_3.value
  except:
      med_brnd_nm = None

  try:
      med_disp_nm = parsed_msg.RXA.RXA_5.RXA_5_4.value
  except:
      med_disp_nm = None

  try:
      dose = parsed_msg.RXA.RXA_6.value
  except:
      dose = None

  try:
      units= parsed_msg.RXA.RXA_7.value
  except:
      units = None

  try:
      admn_prvdr = parsed_msg.RXA.RXA_10.RXA_10_1.value
  except:
      admn_prvdr = None

  try:
      admn_dept = parsed_msg.RXA.RXA_11.value
  except:
      admn_dept = None

  try:
      mar_actn = parsed_msg.RXA.RXA_20.value
  except:
      mar_actn = None

  try:
      med_route = parsed_msg.RXR.RXR_1.value
  except:
      med_route = None
  
  return msg_tm,msg_typ,pat_mrn_id,pat_enc_csn_id,ord_id,ord_stat,ord_admn,ord_qnt,ord_strt_tm,ord_end_tm,typ,ord_crtr,ord_prvdr_id,ord_typ,admn_tm,\
        med_id,med_nm,med_brnd_nm,med_disp_nm,dose,units,admn_prvdr,admn_dept,mar_actn,med_route
  