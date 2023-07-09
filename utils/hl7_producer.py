# Databricks notebook source
# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

connectionString = eventhub_connection_string_publisher
eventHubName = eventhub_feed_name

# COMMAND ----------

sample_hl7_msgs = [
  
  """MSH|^~\\&|Epic|LOVERH|||20220408104652|123884|ADT^A08^ADT_A01|114110|T|2.3\nEVN|A08|20220408104652||REG_UPDATE|123884^CANNON^BRYAN^^^^^^MHCLC^^^^^LOVERH\nPID|1|E1304172217^^^EPI^EPI|E1304172217^^^EPI^EPI||HIDE^TESTSIX^A^^^^D~HIDE^TESTSIX^A^^^^L||19880815|M||cau|123 MAIN ST^^SAINT LOUIS^MO^63129^US^P^^SAINT LOUI|SAINT LOUI|(982)282-9929^P^7^^^982^2829929~^NET^Internet^test@test.com||ENGLISH|SINGLE|||105-02-1858|||NON-HISPANIC||||||||N\nZPD||MYCH||NOT USED|||N|||||||||||N||||||M\nPD1|||PARENT LOVE COUNTY HEALTH CENTER^^50002\nNK1|1|BODY^SOME^^|Sister||(938)398-3839^^7^^^938^3983839||EC1\nNK1|2|||^^^^^US|||EMP||||||NOT EMPLOYED||||||||||||||||||||1003|None\nPV1|1|OUTPATIENT|MHLCRHC^^^LOVERH^^^^^MERCY HEALTH LOVE COUNTY RURAL HEALTH CLINIC^^|EL||||||||||Home/Self|||||510275593|NG||||||||||||||||||||||||20220408100621\nPV2||||||||20220408104500||||Office Visit||||||||||N\nZPV|||||||||||||||||||||||||||||||||||||||(580)276-2400^^^^^580^2762400\nROL|1|||15014076^HUTCHINS^STEPHEN^I^^^^^SOMSER^^^^SOMSER~1073564597^HUTCHINS^STEPHEN^I^^^^^NPI^^^^NPI|20220408104545|20220408104648|||||301 WANDA^^MARIETTA^OK^73448-1229^|(580)276-2400^^8^^^580^2762400~(580)276-4358^^4^^^580^2764358\nOBX|1|CWE|8661-1^CHIEF COMPLAINT:FIND:PT:PATIENT:NOM:REPORTED^LN|1|I10^Essential (primary) hypertension^ICD-10-CM||||||F\nOBX|2|TX|8661-1^CHIEF COMPLAINT:FIND:PT:PATIENT:NOM:REPORTED:18100^LN|2|^^^^^^^^Hypertension||||||F\nOBX|3|CWE|SS003^FACILITY / VISIT TYPE^LN|3|225XN1300X^PRIMARY CARE^NUCC||||||F\nOBX|4|NM|21612-7^AGE TIME PATIENT REPORTED^LN|4|33|a^YEAR^UCUM|||||F|||20220408104652\nOBX|5|TX|740710999^APPT STAFF|1|1073564597^HUTCHINS, STEPHEN I^|||||||||20220408\nOBX|6|TX|740710999^TREATMENT TEAM|1|1073564597^HUTCHINS, STEPHEN I^|||||||||20220408\nAL1|1||12099038^NOT ON FILE^SOMELG\nDG1|1|ICD-10-CM|I10^Essential (primary) hypertension^ICD-10-CM|Essential (primary) hypertension||10800;EPT\nGT1|1|100227348|HIDE^TESTSIX^A^||123 MAIN ST^^SAINT LOUIS^MO^63129^US^^^SAINT LOUI|(982)282-9929^P^7^^^982^2829929||19880815|M|P/F|SLF|105-02-1858||||NOT EMPLOYED|^^^^^US|||None\nIN1|1|339^BCBS|211|BLUE CROSS AND BLUE SHIELD|PO BOX 3028^^TULSA^OK^74101^||||||||||10|HIDE^TESTSIX^A^|Self|19880815|123 MAIN ST^^SAINT LOUIS^MO^63129^US^^^SAINT LOUI|||1|||YES||||||||||234900|JWC39388393||||||None|M|^^^^^US|||BOTH||234900\nIN2||105-02-1858|||Payer Plan||||||||||||||||||||||||||||||||||||||||||||||||||||||||JWC39388393||(982)282-9929^^^^^982^2829929|||||||NOT EMPLOYED\nZIN|||||||||SLF|Self|||100227348|||Current Employment|100+ Employees""",
  
"""MSH|^~\\&|Epic|SJMMC|||20220407132056|94831|ADT^A02^ADT_A02|231945|T|2.3\rEVN|A02|20220407132056||ADT_EVENT|94831^BAKER^KIRSTEN^M^^^^^SJMH^^^^^SJMMC|20220407132000\rPID|1|E1304172010^^^EPI^EPI|E1304172010^^^EPI^EPI||MMM^PATIENTA^A^^^^D~MMM^PATIENTA^A^^^^L||19880301|F|MMM^PATIENTA^^|CAU|123 TESTING RD^^WENTZVILLE^MO^63385^US^P^^SAINT CHAR|SAINT CHAR|(314)555-1234^P^7^^^314^5551234~^NET^Internet^patienta@fake.com~(314)555-1234^P^1^^^314^5551234||ENGLISH|MARRIED|CHR||908-23-0238|||NON-HISPANIC||||||||N\rZPD||MYCH||NOT USED|||N|||||||||||N||||||F\rPD1|||MERCY HOSPITAL ST LOUIS^^20101|1700855780^SMITH^KEVIN^B^^^^^NPI^^^^NPI~1307668^SMITH^KEVIN^B^^^^^SOMSER^^^^SOMSER\rROL|1||PP|1700855780^SMITH^KEVIN^B^^^^^NPI^^^^NPI~1307668^SMITH^KEVIN^B^^^^^SOMSER^^^^SOMSER|20220302||||GENERAL|INTERNAL|664 STONEBROOKS CT^^CHESTERFIELD^MO^63005-4847^|(314)395-3726^^8^^^314^3953726~(314)434-6247^^4^^^314^4346247\rPV1|1|INPATIENT|SJMMED6B^6303^1^SJMMC^Ready^^^^STLO MEDICAL SURGICAL 6^^|EL||SJMMED6B^ASUMS4^1^SJMMC^DIRTY^^^^STLO MEDICAL SURGICAL 6^^|||13228018^JEFN IP^NON-INTEGRATED^PHYSICIAN^^^^^SOMSER^^^^SOMSER|MED||||Clinic|||||510275536|SELF||||||||||||||||||||||||20220407130900||||||21200007533\rPV2||Med/Surg/Gyn||||||20220407||||Hospital Encounter|||||||||2|N||||||||||N|||||||||||||||||||20220407130900|20220407130900\rZPV|||||||||||20220407130900|||||||||2^Hospital Convenience|||||||||||||||||||(314)251-6992^^^^^314^2516992\rROL|2||Referring|1700855780^SMITH^KEVIN^B^^^^^NPI^^^^NPI~1307668^SMITH^KEVIN^B^^^^^SOMSER^^^^SOMSER|||||||664 STONEBROOKS CT^^CHESTERFIELD^MO^63005-4847^|(314)395-3726^^8^^^314^3953726~(314)434-6247^^4^^^314^4346247\rROL|3||Consulting|13228018^JEFN IP^NON-INTEGRATED^PHYSICIAN^^^^^SOMSER^^^^SOMSER\rOBX|1|CWE|8661-1^CHIEF COMPLAINT:FIND:PT:PATIENT:NOM:REPORTED^LN|1|^^^^^^^^ABDOMINAL PAIN||||||F\rOBX|2|CWE|SS003^FACILITY / VISIT TYPE^LN|2|225XN1300X^PRIMARY CARE^NUCC||||||F\rOBX|3|NM|21612-7^AGE TIME PATIENT REPORTED^LN|3|34|a^YEAR^UCUM|||||F|||20220407132056\r"""
  
]

# COMMAND ----------

import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

async def run():
  
    # Create a producer client to send messages to the event hub.
    producer = EventHubProducerClient.from_connection_string(conn_str=connectionString, eventhub_name=eventHubName)
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        
        event_data_batch.add(EventData(sample_hl7_msgs[0]))
        
        event_data_batch.add(EventData(sample_hl7_msgs[1]))
        
#         event_data_batch.add(EventData(sample_hl7_msgs[2]))
        
#         event_data_batch.add(EventData(sample_hl7_msgs[3]))
        
#         event_data_batch.add(EventData(sample_hl7_msgs[4]))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)

loop = asyncio.get_event_loop()
loop.run_until_complete(run())

# COMMAND ----------

