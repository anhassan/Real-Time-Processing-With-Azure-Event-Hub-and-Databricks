# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType
from pyspark.sql.functions import col, udf, explode
from hl7apy import parser
from datetime import datetime
from pytz import timezone
import dateutil.parser

# COMMAND ----------

@udf
def invoke_encntr_dx_process(msg,insert_user_id, msg_enqueued_tsp):

    try:
        jsnmsg = parser.parse_message(
            msg.replace("\n", "\r"), find_groups=False, validation_level=2
        )
    except:
        raise Exception("Invalid message")
    num_cols = 7

    try:
        msg_src = "STL"
        csn = int(jsnmsg.PV1.PV1_19.value) if jsnmsg.PV1.PV1_19.value else None
        msgtime = jsnmsg.MSH.MSH_7.value if jsnmsg.MSH.MSH_7.value else None
        msg_time = str(pytz.timezone("US/Central").localize(dateutil.parser.parse(msgtime)))
    except:
#         return [num_cols * [None] + [insert_user_id]]
        return str({
           "msg_src": None,
           "msg_tm":None,
           "pat_enc_csn_id":None,
           "dx_icd_cd": None,
           "dx_nm": None,
           "dx_cd_typ": None,           
           "insert_user_id": insert_user_id,
           "msg_enqueued_tsp": None
        })
#         return None
#  "row_insert_tsp": None,

    try:
        msg_enqueued_tsp = str(msg_enqueued_tsp.astimezone(pytz.timezone('US/Central')))
    except:
        msg_enqueued_tsp = None

    dx_icd_cd, dx_nm, dx_code = [], [], []
    encntr_dx = []

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
            if (
                identifier_tag == "8661-1"
                and text_tag == "CHIEF COMPLAINT:FIND:PT:PATIENT:NOM:REPORTED"
            ):
                try:
                    dx_icd_cd_tag = (
                        obr.OBX_5.VARIES_1.value if obr.OBX_5.VARIES_1.value else None
                    )
                    dx_icd_cd.append(dx_icd_cd_tag)
                except:
                    dx_icd_cd.append(None)
                try:
                    dx_nm_tag = (
                        obr.OBX_5.VARIES_2.value if obr.OBX_5.VARIES_2.value else None
                    )
                    dx_nm.append(dx_nm_tag)
                except:
                    dx_nm.append(None)
                try:
                    dx_code_tag = (
                        obr.OBX_5.VARIES_3.value if obr.OBX_5.VARIES_3.value else None
                    )
                    dx_code.append(dx_code_tag)
                except:
                    dx_code.append(None)
        except:
            pass

    encntr_dx = [
        {
           "msg_src": msg_src,
           "msg_tm":msg_time,
           "pat_enc_csn_id":int(csn),
           "dx_icd_cd": dx_icd_cd[ind],
           "dx_nm": val,
           "dx_cd_typ": dx_code[ind],
#            "row_insert_tsp": str(datetime.now()),
           "insert_user_id": insert_user_id,
           "msg_enqueued_tsp": msg_enqueued_tsp
        }
        for ind, val in enumerate(dx_nm)
        if val is not None or dx_code[ind] is not None or dx_icd_cd[ind] is not None
    ]
#     encntr_dx = (
#         {num_cols * [None] + [insert_user_id]}
    encntr_dx = [{
           "msg_src": None,
           "msg_tm":None,
           "pat_enc_csn_id":None,
           "dx_icd_cd": None,
           "dx_nm": None,
           "dx_cd_typ": None,
#            "row_insert_tsp": None,
           "insert_user_id": insert_user_id,
           "msg_enqueued_tsp": None
        }]if len(encntr_dx) == 0 else encntr_dx
#     )
    return str(encntr_dx)
#      if len(encntr_dx) == 0 else str(encntr_dx)


# COMMAND ----------

encntr_dx_hist_columns = ["msg_src","msg_tm","pat_enc_csn_id","dx_icd_cd","dx_nm","dx_cd_typ","row_insert_tsp","insert_user_id", "msg_enqueued_tsp"]