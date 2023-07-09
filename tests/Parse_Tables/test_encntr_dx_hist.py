# Databricks notebook source
import hl7apy
from hl7apy import parser
import pytz
import dateutil.parser

# COMMAND ----------

# MAGIC %run ../../code/hl7_sb/encntr_dx_hist

# COMMAND ----------

hl7_sample = """
MSH|^~\&|EPIC|SJM|||20220602160742|39248|ADT^A03^ADT_A03|563782752|P|2.5.1
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

def parse_msg(hl7_sample):
    return parser.parse_message(
        hl7_sample.replace("\n", "\r"), find_groups=False, validation_level=2
    )

# COMMAND ----------

def get_current_user():
  query = 'select current_user' 
  current_user = spark.sql(query)
  return current_user.collect()[0][0]

# COMMAND ----------

def test_encntr_dx_hist(msg, insert_user_id):
    df = spark.createDataFrame(
        [[hl7_sample, insert_user_id]],
        StructType(
            [
                StructField("msg", StringType()),
                StructField("insert_user_id", StringType())
                
            ]
        ),
    ).withColumn(
        "returned",
        invoke_encntr_dx_process(col("msg"), col("insert_user_id")),
    )
    returned = df.collect()[0].returned
    expected = str(
        [
            {
                "msg_src": "STL",
                "msg_tm": "2022-06-02 16:07:42-05:00",
                "pat_enc_csn_id": 248762958,
                "dx_icd_cd": "R00.1",
                "dx_name": "Bradycardia, unspecified",
                "dx_code_type": "ICD-10-CM",
                "insert_user_id": "smhatar1@mercy.net",
            }
        ]
    )

    if returned == expected:
      return "Test Cases Passed Successfully"
    else:
      return "Test Case Failed"
    
insert_user_id = get_current_user()
msg_nm = "1"

test_encntr_dx_hist(hl7_sample, insert_user_id)