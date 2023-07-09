# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType,DecimalType
import dateutil.parser
from dateutil.tz import gettz
import pytz
from datetime import datetime
import dateutil.parser
import json
import hl7
from hl7apy import parser
from hl7apy.core import Group, Segment
from pyspark.sql.functions import *

# COMMAND ----------

def invoke_flwsht_process(row):

    row_dict = row.asDict()
    msg = row_dict["body"]
    insert_user_id = row_dict["insert_user_id"]
    updt_user_id = row_dict["updt_user_id"]
    msg_nm = row_dict["msg_nm"]
    msg_enqueued_tsp = row_dict["enqueued_tsp"]

    
    try:
        jsnmsg = parser.parse_message(msg.replace("\n", "\r"), find_groups=False, validation_level=2)
    except:
        raise Exception("Invalid message")

    try:
      pat_mrn_id = jsnmsg.PID.PID_3.CX_1.value if jsnmsg.PID.PID_3.CX_1.value else None
      pat_enc_csn_id = int(jsnmsg.PV1.PV1_19.value) if jsnmsg.PV1.PV1_19.value else None
      fsd_id = int(jsnmsg.OBR.OBR_3.value[:-14]) if jsnmsg.OBR.OBR_3.value else None
      msgtime = jsnmsg.MSH.MSH_7.value if jsnmsg.MSH.MSH_7.value else None
      msg_tm = pytz.timezone('US/Central').localize(dateutil.parser.parse(msgtime))
      msg_typ = 'ORU'

    except:
       None
    row_obr =[]
    
    for obr in jsnmsg.OBX:
        try:
          flwsht_nm = obr.OBX_3.value[:obr.OBX_3.value.find('^')].partition('&')[0] if obr.OBX_3.value else None
          flwsht_row_num = obr.OBX_3.value[:obr.OBX_3.value.find('^')].partition('&')[2] if obr.OBX_3.value[:obr.OBX_3.value.find('^')].partition('&')[2] else None
          meas_val = obr.OBX_5.value if obr.OBX_5.value else None
          meas_typ = obr.OBX_6.value if obr.OBX_6.value else None
          # rslt_status = obr.OBX_11.value if obr.OBX_11.value else None
          rslttime = obr.OBX_14.value if obr.OBX_14.value else None
          rcrd_tm = pytz.timezone("US/Central").localize(dateutil.parser.parse(rslttime))
          entry_user_id = obr.OBX_16.value.partition('^')[0] if obr.OBX_16.value else None

          row_obr.append([flwsht_nm,flwsht_row_num,meas_val,meas_typ,rcrd_tm,entry_user_id])
          # rslt_val.append(rslt_value)
        except:
          row_obr= ""

              

    flwsht = [[msg_typ,msg_tm,
        msg_nm,
        pat_mrn_id,
        pat_enc_csn_id,
        fsd_id,
        flwsht_nm,flwsht_row_num,meas_val,meas_typ,rcrd_tm,entry_user_id,
        datetime.now(),
        datetime.now(),
        insert_user_id,
        updt_user_id,
        msg_enqueued_tsp
        ] for flwsht_nm,flwsht_row_num,meas_val,meas_typ,rcrd_tm,entry_user_id in  row_obr if len(row_obr) >0]


    # flwsht = [num_cols*[None]] if len(flwsht) == 0 else flwsht
          
    return flwsht
     


# COMMAND ----------

flwsht_columns = ['msg_typ','msg_tm','msg_nm','pat_mrn_id','pat_enc_csn_id','fsd_id','flwsht_nm','flwsht_row_num','meas_val','meas_typ','rcrd_tm','entry_user_id','row_insert_tsp','row_updt_tsp','insert_user_id','updt_user_id','msg_enqueued_tsp']

# COMMAND ----------

flwsht_schema = ArrayType( StructType(
    [
        StructField("msg_typ", StringType(), True),
        StructField("msg_tm", TimestampType(), True),
        StructField("msg_nm", StringType(), True),
        StructField("pat_mrn_id", StringType(), True),
        StructField("pat_enc_csn_id", LongType(), True),
        StructField("fsd_id", LongType(), True),
        StructField("flwsht_nm", StringType(), True),
        StructField("flwsht_row_num", StringType(), True),
        StructField("meas_val", StringType(), True),
        StructField("meas_typ", StringType(), True),
        StructField("rcrd_tm", TimestampType(), True),
        StructField("entry_user_id", StringType(), True),
        StructField("row_insert_tsp", TimestampType(), True),
        StructField("row_updt_tsp", TimestampType(), True),
        StructField("insert_user_id", StringType(), True),
        StructField("updt_user_id", StringType(), True),
        StructField("msg_enqueued_tsp", TimestampType(), True)
    ]
)
)