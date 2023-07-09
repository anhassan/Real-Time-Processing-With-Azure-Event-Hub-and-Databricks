# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook reads messages from Service Bus Topic and write data to Synapse ADT Hist Tables
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 09/01/2022     AHASSAN           Initial creation


# COMMAND ----------

# MAGIC %run ../../code/dtl/adt_dtl

# COMMAND ----------

def get_schema(table):
  return spark.sql("SELECT * FROM {}".format(table)).schema

# COMMAND ----------

schema = "epic_rltm"

adt_hist = "adt_hist_mult_cnsmr"
adt_dtl = "adt_dtl_mult_cnsmr"
encntr_er_complnt = "encntr_er_complnt_mult_cnsmr"
encntr_er_complnt_hist = "encntr_er_complnt_hist_mult_cnsmr"

adt_hist_schema = get_schema("{}.{}".format(schema,adt_hist))
adt_dtl_schema = get_schema("{}.{}".format(schema,adt_dtl))
encntr_er_complnt_schema = get_schema("{}.{}".format(schema,encntr_er_complnt))
encntr_er_complnt_hist_schema = get_schema("{}.{}".format(schema,encntr_er_complnt_hist))

# COMMAND ----------

import dateutil.parser
from datetime import datetime
from pytz import timezone
import pandas as pd
from decimal import *



time_string = "2020-01-01 08:{}"

msg_tm = ["00","05","06","08","10","12","14","16","18"]
msg_src = 10*["STL"]
admit_tm = ["01","06","07","09","11","13","14","17","17"]
hosp_admsn_tm = ["02","07","08","10","12","14","15","18","18"]
hosp_disch_tm = ["55","56","50","42","58","55","56","48","59"]
msg_nm = msg_tm
row_insert_tm = msg_tm
csn = [123,123,456,456,867,867,867,123,456]
csn = [Decimal(val) for val in csn]
trigg_evnt = ["A08","A01","A08","A03","A08","A04","A01","A04","A04"]
rows = []

for index in range(0,len(csn)):
  
  msg_time = dateutil.parser.parse(time_string.format(msg_tm[index])) 
  admit_time = dateutil.parser.parse(time_string.format(admit_tm[index])) if admit_tm[index] is not None else None
  hosp_admsn_time = dateutil.parser.parse(time_string.format(hosp_admsn_tm[index])) if hosp_admsn_tm[index] is not None else None
  hosp_disch_time = dateutil.parser.parse(time_string.format(hosp_disch_tm[index])) if hosp_disch_tm[index] is not None else None
  row_insert_tsp = dateutil.parser.parse(time_string.format(row_insert_tm[index])) if row_insert_tm[index] is not None else None
  
  row_data = [None,msg_src[index],trigg_evnt[index],msg_time,msg_nm[index],None,None,csn[index]] + 3*[None] + [hosp_admsn_time,hosp_disch_time] \
              + 10*[None] + [admit_time] + 3*[None] + [row_insert_tsp,None]
  
  rows.append(row_data)

df1 = pd.DataFrame(rows,columns=adt_hist_schema.names)

  
df1

# COMMAND ----------


msg_tm = ["20","21","22","25","26","27"]
admit_tm = ["21","22","23","26","27","28"]
hosp_admsn_tm = ["22","23","26","27","28","30"]
hosp_disch_tm = ["35","38","39","45","44","40"]
msg_nm = msg_tm
row_insert_tm = msg_tm
csn = [123,456,867,911,911,123]
csn = [Decimal(val) for val in csn]
msg_src = 6*["STL"]
trigg_evnt = ["A03","A13","A11","A01","A04","A08"]
rows = []

for index in range(0,len(csn)):
  
  msg_time = dateutil.parser.parse(time_string.format(msg_tm[index])) 
  admit_time = dateutil.parser.parse(time_string.format(admit_tm[index])) if admit_tm[index] is not None else None
  hosp_admsn_time = dateutil.parser.parse(time_string.format(hosp_admsn_tm[index])) if hosp_admsn_tm[index] is not None else None
  hosp_disch_time = dateutil.parser.parse(time_string.format(hosp_disch_tm[index])) if hosp_disch_tm[index] is not None else None
  row_insert_tsp = dateutil.parser.parse(time_string.format(row_insert_tm[index])) if row_insert_tm[index] is not None else None
  
  row_data = [None,msg_src[index],trigg_evnt[index],msg_time,msg_nm[index],None,None,csn[index]] + 3*[None] + [hosp_admsn_time,hosp_disch_time] \
              + 10*[None] + [admit_time] + 3*[None] + [row_insert_tsp,None]
  
  rows.append(row_data)

df2 = pd.DataFrame(rows,columns=adt_hist_schema.names)

  
df2

# COMMAND ----------


msg_tm = ["30","31","32"]
admit_tm = ["31","32","36"]
hosp_admsn_tm = ["32","33","37"]
hosp_disch_tm = ["33","34","38"]
msg_nm = msg_tm
row_insert_tm = msg_tm
csn = [Decimal(123),Decimal(911),Decimal(123)]
msg_src = 3*["STL"]
trigg_evnt = ["A04","A04","A04"]
rows = []


for index in range(0,len(csn)):
  
  msg_time = dateutil.parser.parse(time_string.format(msg_tm[index])) 
  admit_time = dateutil.parser.parse(time_string.format(admit_tm[index])) if admit_tm[index] is not None else None
  hosp_admsn_time = dateutil.parser.parse(time_string.format(hosp_admsn_tm[index])) if hosp_admsn_tm[index] is not None else None
  hosp_disch_time = dateutil.parser.parse(time_string.format(hosp_disch_tm[index])) if hosp_disch_tm[index] is not None else None
  row_insert_tsp = dateutil.parser.parse(time_string.format(row_insert_tm[index])) if row_insert_tm[index] is not None else None
  
  row_data = [None,msg_src[index],trigg_evnt[index],msg_time,msg_nm[index],None,None,csn[index]] + 3*[None] + [hosp_admsn_time,hosp_disch_time] \
              + 10*[None] + [admit_time] + 3*[None] + [row_insert_tsp,None]
  
  rows.append(row_data)

df3 = pd.DataFrame(rows,columns=adt_hist_schema.names)

  
df3

# COMMAND ----------

batch1 = spark.createDataFrame(df1,adt_hist_schema)
batch2 = spark.createDataFrame(df2,adt_hist_schema)
batch3 = spark.createDataFrame(df3,adt_hist_schema)

input_batchs = [batch1,batch2,batch3]

# COMMAND ----------

import dateutil.parser
import pandas as pd

expected_cols = ["pat_enc_csn_id","adt_arrival_time","hosp_admsn_time","hosp_disch_time"]

expected_batch1_transformed = pd.DataFrame(
                               [[Decimal(123)] + [dateutil.parser.parse(tm) for tm in ["2020-01-01T08:17:00","2020-01-01T08:07:00"]] + [pd.Timestamp('NaT').to_pydatetime()],\
                               [Decimal(456),dateutil.parser.parse("2020-01-01 08:17:00"), pd.Timestamp('NaT').to_pydatetime() ,dateutil.parser.parse("2020-01-01 08:42:00")],\
                               [Decimal(867)] + [dateutil.parser.parse(tm) for tm in ["2020-01-01 08:13:00","2020-01-01 08:15:00"]] + [pd.Timestamp('NaT').to_pydatetime()]],
                               columns = expected_cols)

expected_batch2_transformed = pd.DataFrame(
                               [[Decimal(123)] + 2*[pd.Timestamp('NaT').to_pydatetime()] + [dateutil.parser.parse("2020-01-01 08:35:00")],
                               [Decimal(456)] + 3*[pd.Timestamp('NaT').to_pydatetime()],
                               [Decimal(867)] + 3*[pd.Timestamp('NaT').to_pydatetime()],
                               [Decimal(911)] + [dateutil.parser.parse(tm) for tm in ["2020-01-01 08:27:00","2020-01-01 08:27:00"]] + [pd.Timestamp('NaT').to_pydatetime()]],
                               columns = expected_cols)

expected_batch3_transformed = pd.DataFrame(
                               [[Decimal(123),dateutil.parser.parse("2020-01-01 08:36:00")] + 2*[pd.Timestamp('NaT').to_pydatetime()],
                               [Decimal(911),dateutil.parser.parse("2020-01-01 08:32:00")] + 2*[pd.Timestamp('NaT').to_pydatetime()]],
                               columns = expected_cols)

expected_transformed_batchs = [expected_batch1_transformed,expected_batch2_transformed,expected_batch3_transformed]

# COMMAND ----------

import dateutil.parser
import pandas as pd

expected_cols = ["pat_enc_csn_id","adt_arrival_time","hosp_admsn_time","hosp_disch_time"]

expected_cdc_batch1 = pd.DataFrame(
                               [[Decimal(123)] + [dateutil.parser.parse(tm) for tm in ["2020-01-01T08:17:00","2020-01-01T08:07:00"]] + [pd.Timestamp('NaT').to_pydatetime()],\
                               [Decimal(456),dateutil.parser.parse("2020-01-01 08:17:00"), pd.Timestamp('NaT').to_pydatetime() ,dateutil.parser.parse("2020-01-01 08:42:00")],\
                               [Decimal(867)] + [dateutil.parser.parse(tm) for tm in ["2020-01-01 08:13:00","2020-01-01 08:15:00"]] + [pd.Timestamp('NaT').to_pydatetime()]],\
                               columns = expected_cols)

expected_cdc_batch2 = pd.DataFrame(
                               [[Decimal(123)] + [dateutil.parser.parse(tm) for tm in ["2020-01-01T08:17:00","2020-01-01T08:07:00","2020-01-01T08:35:00"]],\
                               [Decimal(456),dateutil.parser.parse("2020-01-01 08:17:00")]+ 2*[pd.Timestamp('NaT').to_pydatetime()],\
                               [Decimal(867),dateutil.parser.parse("2020-01-01 08:13:00")]+ 2*[pd.Timestamp('NaT').to_pydatetime()],\
                               [Decimal(911)] + 2*[dateutil.parser.parse("2020-01-01T08:27:00")] + [pd.Timestamp('NaT').to_pydatetime()]],
                               columns = expected_cols)

expected_cdc_batch3 = pd.DataFrame(
                               [[Decimal(123)] + [dateutil.parser.parse(tm) for tm in ["2020-01-01T08:36:00","2020-01-01T08:07:00","2020-01-01T08:35:00"]],\
                               [Decimal(456),dateutil.parser.parse("2020-01-01 08:17:00")] +  2*[pd.Timestamp('NaT').to_pydatetime()],\
                               [Decimal(867),dateutil.parser.parse("2020-01-01 08:13:00")]  + 2*[pd.Timestamp('NaT').to_pydatetime()],\
                               [Decimal(911)]+ [dateutil.parser.parse(tm) for tm in ["2020-01-01T08:32:00","2020-01-01T08:27:00"]] + [pd.Timestamp('NaT').to_pydatetime()]],
                               columns = expected_cols)

expected_cdc_batchs = [expected_cdc_batch1,expected_cdc_batch2,expected_cdc_batch3]

# COMMAND ----------

import dateutil.parser
import pandas as pd
from decimal import *

time_string = "2020-01-01 09:{}"


batch1_child = spark.createDataFrame(
               pd.DataFrame(
               [["STL",dateutil.parser.parse(time_string.format("01")),Decimal(123),"ER1",dateutil.parser.parse(time_string.format("01")),None],
               ["STL",dateutil.parser.parse(time_string.format("02")),Decimal(456),"ER2",dateutil.parser.parse(time_string.format("02")),None],
               ["STL",dateutil.parser.parse(time_string.format("03")),Decimal(123),"ER3",dateutil.parser.parse(time_string.format("03")),None]],
               columns = encntr_er_complnt_hist_schema.names),
               encntr_er_complnt_hist_schema)

batch2_child = spark.createDataFrame(
                pd.DataFrame(
                [["STL",dateutil.parser.parse(time_string.format("05")),Decimal(123),"ER8",dateutil.parser.parse(time_string.format("05")),None],
                ["STL",dateutil.parser.parse(time_string.format("06")),Decimal(897),"ER9",dateutil.parser.parse(time_string.format("06")),None]],
                columns = encntr_er_complnt_hist_schema.names),
                encntr_er_complnt_hist_schema)

input_batchs_child = [batch1_child,batch2_child]

expected_cdc_batch1_child = pd.DataFrame(
               [["STL",dateutil.parser.parse(time_string.format("03")),Decimal(123),"ER3",dateutil.parser.parse(time_string.format("03")),pd.Timestamp('NaT').to_pydatetime(),None,None],
               ["STL",dateutil.parser.parse(time_string.format("02")),Decimal(456),"ER2",dateutil.parser.parse(time_string.format("02")),pd.Timestamp('NaT').to_pydatetime(),None,None]],
               columns = encntr_er_complnt_schema.names)

expected_cdc_batch2_child = pd.DataFrame(
               [["STL",dateutil.parser.parse(time_string.format("05")),Decimal(123),"ER8",dateutil.parser.parse(time_string.format("05")),pd.Timestamp('NaT').to_pydatetime(),None,None],
               ["STL",dateutil.parser.parse(time_string.format("02")),Decimal(456),"ER2",dateutil.parser.parse(time_string.format("02")),pd.Timestamp('NaT').to_pydatetime(),None,None],
               ["STL",dateutil.parser.parse(time_string.format("06")),Decimal(897),"ER9",dateutil.parser.parse(time_string.format("06")),pd.Timestamp('NaT').to_pydatetime(),None,None]],
               columns = encntr_er_complnt_schema.names)

expected_cdc_batchs_child = [expected_cdc_batch1_child,expected_cdc_batch2_child]

# COMMAND ----------

from pandas.testing import assert_frame_equal

def test_get_transformed_batch(input_batchs,expected_batchs):
  for index,input_batch in enumerate(input_batchs):
    actual_batch = get_transformed_batch(input_batch).select("pat_enc_csn_id","adt_arrival_time","hosp_admsn_time","hosp_disch_time")\
                                                          .toPandas()
    assert_frame_equal(expected_batchs[index],actual_batch)
  print("Unit Test Succeeded for test_get_transformed_batch...")
  

# COMMAND ----------

from delta import *

def test_captureChangeData(dtl_table_name,dtl_table_schema,input_batchs,expected_batchs):
  dtl_table_name = dtl_table_name + "_unit_test"
  spark.createDataFrame([],dtl_table_schema).write.format("delta").mode("overwrite").saveAsTable(dtl_table_name)
  dtl_table_loc = "dbfs:/user/hive/warehouse/{}".format(dtl_table_name)
  dtl_df = DeltaTable.forPath(spark,dtl_table_loc)
  
  for index,input_batch in enumerate(input_batchs):
    captureChangeData(input_batch,index,dtl_df,dtl_table_name)
    
    actual_batch = spark.sql("SELECT * FROM {}".format(dtl_table_name))\
                        .select("pat_enc_csn_id","adt_arrival_time","hosp_admsn_time","hosp_disch_time")\
                        .toPandas()
    assert_frame_equal(expected_batchs[index],actual_batch)
  print("Unit Test Succeeded for test_captureChangeData...")
    

# COMMAND ----------

def test_captureChangeDataChild(dtl_table_name,input_batchs,expected_batchs):

  dtl_table_name = "encntr_er_complnt" + "_unit_test"
  dtl_table = spark.createDataFrame([],encntr_er_complnt_schema).write.format("delta").mode("overwrite").saveAsTable(dtl_table_name)
  for index,input_batch in enumerate(input_batchs):
    captureChangeDataChildOptimizedDeletes(input_batch,index,None,None,dtl_table_name)
    actual_batch = spark.sql("SELECT * FROM {}".format(dtl_table_name))\
                                               .orderBy(col("pat_enc_csn_id").asc())\
                                               .toPandas()
    assert_frame_equal(expected_batchs[index],actual_batch)
  print("Unit Test Succeeded for test_captureChangeDataChild...")
    

# COMMAND ----------

test_get_transformed_batch(input_batchs,expected_transformed_batchs)

# COMMAND ----------

test_captureChangeData(adt_dtl,adt_dtl_schema,input_batchs,expected_cdc_batchs)

# COMMAND ----------

test_captureChangeDataChild(encntr_er_complnt,input_batchs_child,expected_cdc_batchs_child)