# Databricks notebook source
# MAGIC %run ./test_data_script

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

def get_hist_col(df_retained,col_name):
  window = Window.partitionBy("pat_enc_csn_id").orderBy(col("msg_tm").desc())
  
  return df_retained.where(col(col_name).isNotNull())\
                    .withColumn("row_number",row_number().over(window))\
                    .where(col("row_number")==1)\
                    .select("pat_enc_csn_id","pat_mrn_id",col_name)
    

# COMMAND ----------

def get_hist_cols(df,hist_cols):
  join_cols = ["pat_enc_csn_id","pat_mrn_id"]
  filter_cols = hist_cols + join_cols
  hists_df = [get_hist_col(df,hist_col) for hist_col in hist_cols]
  if len(hists_df) > 1:
    df = hists_df[0]
    for hist_df in hists_df[1:]:
      df = df.join(hist_df,join_cols)
    df = df.select(*filter_cols)
    return df
  elif len(hists_df) == 1:
    return hists_df[0]
  else:
    raise Exception("Please provide a hist column")
  

# COMMAND ----------

def get_actual_data_adt_hist(table_name):
  window = Window.partitionBy("pat_enc_csn_id").orderBy(col("msg_tm").desc())
  hist_cols = ["adt_arrival_time","hosp_admsn_time","hosp_disch_time"]
  join_cols = ["pat_enc_csn_id","pat_mrn_id"]
  
  df = spark.sql("SELECT * FROM {}".format(table_name))
  df_retained = df.withColumn("threshold_date",current_date()-1)\
                  .where(to_date(col("msg_tm"),"yyyy-MM-dd") == col("threshold_date"))
  
  df_latest = df_retained.withColumn("row_number",row_number().over(window))\
                         .where(col("row_number")==1)\
                         .drop(*hist_cols)
  
  df_adt_data = df_latest.join(get_hist_cols(df,hist_cols),join_cols,"left")
  
  return df_adt_data


# COMMAND ----------

def get_actual_data_child_hist(df_adt,table_name):
  df_child = spark.sql("SELECT * FROM {}".format(table_name))
  filter_cols = df_child.columns
  return df_adt.select("pat_enc_csn_id")\
               .join(df_child,["pat_enc_csn_id"])\
               .select(*filter_cols)

# COMMAND ----------

from pyspark.sql.functions import *

def get_match_report(df_actual,df_test,join_cols):
  condition_cols = df_test.columns
  conditions = [when(df_actual[column]!=df_test[column],lit(column)).otherwise(lit("")) for column in condition_cols if column not in join_cols]
  select_clause = [col(column) for column in join_cols] + [array_remove(array(*conditions),"").alias("mismatched_cols")]
  
  df_report = df_actual.join(df_test,join_cols)\
                       .select(*select_clause)
  df_mismatches = df_report.where(size(col("mismatched_cols")) > 0)
  
  mismatches_count = df_mismatches.count()
  matches_count = df_report.count() - mismatches_count
  
  df_actual_extras = df_actual.join(df_test,join_cols,"left_anti")
  df_test_extras = df_test.join(df_actual,join_cols,"left_anti")
  
  print("Total matched rows : {} ".format(matches_count))
  print("Total mismatched rows : {} ".format(mismatches_count))
  print("Row count for records that are in actual dataset but not in test dataset : {} ".format(df_actual_extras.count()))
  print("Row count for records that are in test dataset but not in actual dataset : {} ".format(df_test_extras.count()))
  
  print("Mismathced Rows")
  display(df_mismatches)
  print("Extra Rows for Actual Stream Data not in Test Data")
  display(df_actual_extras)
  print("Extra Rows for Test Data not in Actual Stream Data")
  display(df_test_extras)
  
  return df_mismatches,df_actual_extras,df_test_extras

# COMMAND ----------

def run_integration_test(table_name):
  [schema,table] = table_name.split(".")
  adt_type = "{}.{}".format(schema,"adt_hist") if "hist" in table else "{}.{}".format(schema,"adt_dtl")
  adt_table_name = adt_type + table[table.rfind("_"):]if "_evh" in table or "_sb" in table else adt_type
  df_adt = get_actual_data_adt_hist(adt_table_name)
  df_adt_test = get_adt_hist_test_data()
  
  join_cols = ["pat_enc_csn_id","pat_mrn_id"]
  join_cols_child = ["pat_enc_csn_id"]
  
  if "adt" in table_name:
    return get_match_report(df_adt,df_adt_test,join_cols)
  
  if "encntr_dx" in table_name:
    df_test = get_encntr_dx_hist_test_data(df_adt_test)
    df_actual = get_actual_data_child_hist(df_adt,table_name)
    return get_match_report(df_actual,df_test,join_cols_child)
  
  if "encntr_er_complnt" in table_name:
    df_test = get_encntr_er_complnt_hist_test_data(df_adt_test)
    df_actual = get_actual_data_child_hist(df_adt,table_name)
    return get_match_report(df_actual,df_test,join_cols_child)
  
  if "encntr_visit_rsn" in table_name:
    df_test = get_encntr_visit_rsn_hist_test_data(df_adt_test)
    df_actual = get_actual_data_child_hist(df_adt,table_name)
    return get_match_report(df_actual,df_test,join_cols_child)
  
  

# COMMAND ----------

df_mismatches,df_actual_extras,df_test_extras = run_integration_test("epic_rltm.adt_hist")

# COMMAND ----------

df_mismatches,df_actual_extras,df_test_extras = run_integration_test("epic_rltm.encntr_dx_hist")

# COMMAND ----------

df_mismatches_2,df_actual_extras_2,df_test_extras_2 = run_integration_test("epic_rltm.encntr_er_complnt_hist")

# COMMAND ----------

df_mismatches_3,df_actual_extras_3,df_test_extras_3 = run_integration_test("epic_rltm.encntr_visit_rsn_hist")

# COMMAND ----------

