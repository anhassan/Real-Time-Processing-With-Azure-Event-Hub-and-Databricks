# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType,TimestampType,LongType,ArrayType
from pyspark.sql.functions import col, udf, explode
from hl7apy import parser
from datetime import datetime
from pytz import timezone
import dateutil.parser

# COMMAND ----------

def invoke_encntr_dx_process(row):

    row_dict = row.asDict()
    msg = row_dict["body"]
    insert_user_id = row_dict["insert_user_id"]

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
        msg_time = pytz.timezone("US/Central").localize(dateutil.parser.parse(msgtime))
    except:
        return [num_cols * [None] + [insert_user_id]]

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
        [
            msg_src,
            msg_time,
            int(csn),
            dx_icd_cd[ind],
            val,
            dx_code[ind],
            datetime.now(),
            insert_user_id
        ]
        for ind, val in enumerate(dx_nm)
        if val is not None or dx_code[ind] is not None or dx_icd_cd[ind] is not None
    ]
    encntr_dx = (
        [num_cols * [None] + [insert_user_id]]
        if len(encntr_dx) == 0
        else encntr_dx
    )
    return encntr_dx


# COMMAND ----------

encntr_dx_hist_schema = ArrayType(
    StructType(
        [
            StructField("msg_src", StringType(), True),
            StructField("msg_tm", TimestampType(), True),
            StructField("pat_enc_csn_id", LongType(), True),
            StructField("dx_icd_cd", StringType(), True),
            StructField("dx_nm", StringType(), True),
            StructField("dx_cd_typ", StringType(), True),
            StructField("row_insert_tsp", TimestampType(), True),
            StructField("insert_user_id", StringType(), True),
            
        ]
    )
)

# COMMAND ----------

encntr_dx_hist_columns = ["msg_src","msg_tm","pat_enc_csn_id","dx_icd_cd","dx_nm","dx_cd_typ","row_insert_tsp","insert_user_id"]