# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, ArrayType, DecimalType
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

def get_acuity_level(message):
    delimiter = "|||"
    acuity_level = ""
    for obr in message.OBX:
        try:
            identifier_tag = obr.OBX_3.CE_1.value if obr.OBX_3.CE_1.value else None
        except:
            identifier_tag = None
        try:
            text_tag = obr.OBX_3.CE_2.value if obr.OBX_3.CE_2.value else None
        except:
            text_tag = None
        try:
            acuity_tag = obr.OBX_5.VARIES_2.value if obr.OBX_5.VARIES_2.value else None
        except:
            acuity_tag = None
        try:
            if identifier_tag == "11283-9" and text_tag == "ACUITY ASSESSMENT AT FIRST ENCOUNTER":
                acuity_value = acuity_tag
                if len(acuity_level) == 0 :
                    acuity_level += acuity_value
                else:
                    acuity_level = acuity_level + delimiter + acuity_value
        except:
            pass
    acuity_level_filtered = None if len(acuity_level) == 0 else acuity_level
    return acuity_level_filtered

# COMMAND ----------

def get_adt_arrival_time(message):
    try:
        trigger_type = message.MSH.MSH_9.MSG_2.value if message.MSH.MSH_9.MSG_2.value else None
    except:
        trigger_type = None 
    try:
        adt_arrival_time = message.PV1.PV1_44.value if message.PV1.PV1_44.value else None
    except:
        adt_arrival_time = None  
    if trigger_type == "A04":
        try:
            adt_arrival_time_filtered = str(pytz.timezone('US/Central').localize(dateutil.parser.parse(adt_arrival_time))) if adt_arrival_time else None
            return adt_arrival_time_filtered
        except:
            return None
    return None

# COMMAND ----------

def get_hosp_admsn_time(message):
    try:
        trigger_type = message.MSH.MSH_9.MSG_2.value if message.MSH.MSH_9.MSG_2.value else None
    except:
        trigger_type = None
    try:
        hosp_admsn_time = message.PV1.PV1_44.value if message.PV1.PV1_44.value else None
    except:
        hosp_admsn_time = None
    if trigger_type == "A01" or trigger_type == "A11":
        try:
            hosp_admsn_time_filtered = hosp_admsn_time if hosp_admsn_time is None else str(pytz.timezone('US/Central').localize(dateutil.parser.parse(hosp_admsn_time)))
            return hosp_admsn_time_filtered
        except:
            return None 
    return None

# COMMAND ----------

def get_hosp_disch_time(message):
    try:
        trigger_type = message.MSH.MSH_9.MSG_2.value if message.MSH.MSH_9.MSG_2.value else None
    except:
        trigger_type = None    
    try:
        hosp_disch_time = message.PV1.PV1_45.value if message.PV1.PV1_45.value else None
    except:
        hosp_disch_time = None 
    if trigger_type == "A03" or trigger_type == "A13":
        try:
            hosp_disch_time_filtered = hosp_disch_time if hosp_disch_time is None else str(pytz.timezone("US/Central").localize(dateutil.parser.parse(hosp_disch_time)))
            return hosp_disch_time_filtered
        except:
            return None 
    return None

# COMMAND ----------

@udf
def invoke_adt_hist_process(msg, insert_user_id, msg_nm, msg_enqueued_tsp):

    try:
        jsnmsg = parser.parse_message(msg.replace('\n','\r'), find_groups = False, validation_level=2)
    except:
        raise Exception("Invalid message")

    try:
        MessageCode = (
            jsnmsg.MSH.MSH_9.MSG_1.value if jsnmsg.MSH.MSH_9.MSG_1.value else None
        )
    except:
        MessageCode = None

    OriginatingRegion = "STL"

    try:
        msgtime = jsnmsg.MSH.MSH_7.value if jsnmsg.MSH.MSH_7.value else None
        msg_time = str(pytz.timezone("US/Central").localize(dateutil.parser.parse(msgtime)))

    except:
        msg_time = None

    try:
        TriggerEvent = (
            jsnmsg.MSH.MSH_9.MSG_2.value if jsnmsg.MSH.MSH_9.MSG_2.value else None
        )
    except:
        TriggerEvent = None

    try:
        BODID = jsnmsg.MSH.MSH_10.value if jsnmsg.MSH.MSH_10.value else None
    except:
        BODID = None

    try:
        pat_mrn_id = (
            jsnmsg.PID.PID_3.CX_1.value if jsnmsg.PID.PID_3.CX_1.value else None
        )
    except:
        pat_mrn_id = None

    try:
        csn_id = int(jsnmsg.PV1.PV1_19.value) if jsnmsg.PV1.PV1_19.value else None

    except:
        csn_id = None

    try:
        dob = jsnmsg.PID.PID_7.value if jsnmsg.PID.PID_7.value else None
        DatetimeOfBirth = str(pytz.timezone("US/Central").localize(
            dateutil.parser.parse(dob)
        ))
    except:
        DatetimeOfBirth = None

    try:
        dod = jsnmsg.PID.PID_29.value if jsnmsg.PID.PID_29.value else None
        PatientDeathDateAndTime = str(pytz.timezone("US/Central").localize(
            dateutil.parser.parse(dod)
        ))
    except:
        PatientDeathDateAndTime = None

    try:
        PatientClass = jsnmsg.PV1.PV1_2.value if jsnmsg.PV1.PV1_2.value else None
    except:
        PatientClass = None

    try:
        PointOfCare = (
            jsnmsg.PV1.PV1_3.PL_1.value if jsnmsg.PV1.PV1_3.PL_1.value else None
        )
    except:
        PointOfCare = None

    try:
        HierarchicDesignator = (
            jsnmsg.EVN.EVN_5.XCN_14.value if jsnmsg.EVN.EVN_5.XCN_14.value else None
        )
    except:
        HierarchicDesignator = None

    try:
        Room = jsnmsg.PV1.PV1_3.PL_2.value if jsnmsg.PV1.PV1_3.PL_2.value else None
    except:
        Room = None

    try:
        Bed = jsnmsg.PV1.PV1_3.PL_3.value if jsnmsg.PV1.PV1_3.PL_3.value else None
    except:
        Bed = None

    try:
        LocationStatus = (
            jsnmsg.PV1.PV1_3.PL_5.value if jsnmsg.PV1.PV1_3.PL_5.value else None
        )
    except:
        LocationStatus = None

    try:
        sex = jsnmsg.PID.PID_8.value if jsnmsg.PID.PID_8.value else None
    except:
        sex = None

    try:

        ModeOfArrivalCode = jsnmsg.PV2.PV2_38.value if jsnmsg.PV2.PV2_38.value else None
    except:
        ModeOfArrivalCode = None

    try:

        EDDisposition = jsnmsg.ZPV.ZPV_15.value if jsnmsg.ZPV.ZPV_15.value else None
    except:
        EDDisposition = None

    try:

        DischargeDisposition = (
            jsnmsg.PV1.PV1_36.value if jsnmsg.PV1.PV1_36.value else None
        )
    except:
        DischargeDisposition = None

    try:
        hsp_account_id = (
            int(jsnmsg.PV1.PV1_50.value) if jsnmsg.PV1.PV1_50.value else None
        )

    except:
        hsp_account_id = None

    try:
        AccommodationCode = jsnmsg.PV2.PV2_2.value if jsnmsg.PV2.PV2_2.value else None
    except:
        AccommodationCode = None

    try:
        OperatorID = (
            jsnmsg.EVN.EVN_5.XCN_1.value if jsnmsg.EVN.EVN_5.XCN_1.value else None
        )
    except:
        OperatorID = None

    try:
        msg_enqueued_tsp = str(msg_enqueued_tsp.astimezone(pytz.timezone('US/Central')))
    except:
        msg_enqueued_tsp = None

    try:
        pregnancy_flg = jsnmsg.ZPD.ZPD_7.value if str(jsnmsg.ZPD.ZPD_7.value).upper() == 'Y' else None
    except:
        pregnancy_flg = None

    hl7_parsed = {
        "msg_typ": MessageCode,
        "msg_src":OriginatingRegion,
        "trigger_evnt":TriggerEvent,
        "msg_tm": msg_time,
        "msg_nm":msg_nm,
        "bod_id":BODID,
        "pat_mrn_id":pat_mrn_id,
        "pat_enc_csn_id": csn_id,
        "birth_dt":DatetimeOfBirth,
        "death_dt":PatientDeathDateAndTime,
        "pat_class_abbr":PatientClass,
        "hosp_admsn_tm":get_hosp_admsn_time(jsnmsg),
        "hosp_disch_tm":get_hosp_disch_time(jsnmsg),
        "dept_abbr":PointOfCare,
        "loc_abbr": HierarchicDesignator,
        "room_nm":Room,
        "bed_label":Bed,
        "bed_stat":LocationStatus,
        "sex_abbr":sex,
        "means_of_arrv_abbr":ModeOfArrivalCode,
        "acuity_level_abbr":get_acuity_level(jsnmsg),
        "ed_dsptn_abbr":EDDisposition,
        "disch_dsptn_abbr":DischargeDisposition,
        "adt_arvl_tm":get_adt_arrival_time(jsnmsg),
        "hsp_acct_id":hsp_account_id,
        "accommodation_abbr":AccommodationCode,
        "user_id":OperatorID,
        "insert_user_id": insert_user_id,
        "msg_enqueued_tsp": msg_enqueued_tsp,
        "pregnancy_flg": pregnancy_flg
      }
    return str(hl7_parsed)


# COMMAND ----------

adt_hist_columns = ['msg_typ','msg_src','trigger_evnt','msg_tm','msg_nm','bod_id','pat_mrn_id','pat_enc_csn_id','birth_dt','death_dt','pat_class_abbr',
                    'hosp_admsn_tm','hosp_disch_tm','dept_abbr','loc_abbr','room_nm','bed_label','bed_stat','sex_abbr','means_of_arrv_abbr',
                    'acuity_level_abbr','ed_dsptn_abbr','disch_dsptn_abbr','adt_arvl_tm','hsp_acct_id','accommodation_abbr','user_id',
                    'row_insert_tsp','insert_user_id', 'msg_enqueued_tsp', 'pregnancy_flg']