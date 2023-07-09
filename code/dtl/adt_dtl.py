# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook reads messages from Service Bus Topic and write data to Synapse ADT Hist Tables
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 09/01/2022     AHASSAN           Fixed adt_arrival_time logic


# COMMAND ----------

# MAGIC %run ../../utils/utilities

# COMMAND ----------

spark.conf.set("spark.sql.files.ignoreMissingFiles",True)

# COMMAND ----------

from pyspark.sql.types import *

adt_hist_schema = StructType(
    [
        StructField("msg_typ", StringType(), True),
        StructField("msg_src", StringType(), True),
        StructField("trigger_evnt", StringType(), True),
        StructField("msg_tm", StringType(), True),
        StructField("msg_nm", StringType(), True),
        StructField("bod_id", StringType(), True),
        StructField("pat_mrn_id", StringType(), True),
        StructField("pat_enc_csn_id", StringType(), True),
        StructField("birth_dt", StringType(), True),
        StructField("death_dt", StringType(), True),
        StructField("pat_class_abbr", StringType(), True),
        StructField("hosp_admsn_tm", StringType(), True),
        StructField("hosp_disch_tm", StringType(), True),
        StructField("dept_abbr", StringType(), True),
        StructField("loc_abbr", StringType(), True),
        StructField("room_nm", StringType(), True),
        StructField("bed_label", StringType(), True),
        StructField("bed_stat", StringType(), True),
        StructField("sex_abbr", StringType(), True),
        StructField("means_of_arrv_abbr", StringType(), True),
        StructField("acuity_level_abbr", StringType(), True),
        StructField("ed_dsptn_abbr", StringType(), True),
        StructField("disch_dsptn_abbr", StringType(), True),
        StructField("adt_arvl_tm", StringType(), True),
        StructField("hsp_acct_id", StringType(), True),  # Long
        StructField("accommodation_abbr", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("insert_user_id", StringType(), True),
        StructField("msg_enqueued_tsp", StringType(), True),
        StructField("pregnancy_flg", StringType(), True),
        StructField("row_insert_tsp",StringType(),True)
    ]
)

encntr_dx_hist_schema = ArrayType(
    StructType(
        [
            StructField("msg_src", StringType(), True),
            StructField("msg_tm", StringType(), True),
            StructField("pat_enc_csn_id", StringType(), True),
            StructField("dx_icd_cd", StringType(), True),
            StructField("dx_nm", StringType(), True),
            StructField("dx_cd_typ", StringType(), True),
            StructField("insert_user_id", StringType(), True),
            StructField("msg_enqueued_tsp", StringType(), True)
            
        ]
    )
)

encntr_er_complnt_hist_schema = StructType(
    [
        StructField("msg_src", StringType(), True),
        StructField("msg_tm", StringType(), True),
        StructField("pat_enc_csn_id", StringType(), True),
        StructField("er_complnt", StringType(), True),
        StructField("insert_user_id", StringType(), True),
        StructField("msg_enqueued_tsp", StringType(), True)
        
    ]
)

encntr_visit_rsn_hist_schema = ArrayType(
    StructType(
        [
            StructField("msg_src", StringType(), True),
            StructField("msg_tm", StringType(), True),
            StructField("pat_enc_csn_id", StringType(), True),
            StructField("encntr_rsn", StringType(), True),
            StructField("insert_user_id", StringType(), True),
            StructField("msg_enqueued_tsp", StringType(), True)
        ]
    )
)

encntr_nte_hist_schema = StructType(
    StructType(
        [
            StructField("msg_src", StringType(), True),
            StructField("msg_tm", StringType(), True),
            StructField("pat_enc_csn_id", StringType(), True),
            StructField("nte_txt", StringType(), True),
            StructField("nte_typ", StringType(), True),
            StructField("insert_user_id", StringType(), True),
            StructField("msg_enqueued_tsp", StringType(), True)
        ]
    )
)

# COMMAND ----------

select_sequence = {
  "epic_rltm.adt_dtl" : [ "msg_typ", "msg_src", "trigger_evnt", "msg_tm", "msg_nm", "bod_id", "pat_mrn_id", "pat_enc_csn_id", "birth_dt",  "death_dt", "pat_class_abbr" ,"hosp_admsn_tm", "hosp_disch_tm", "dept_abbr", "loc_abbr", "room_nm", "bed_label", "bed_stat", "sex_abbr", "means_of_arrv_abbr", "acuity_level_abbr", "ed_dsptn_abbr", "disch_dsptn_abbr", "adt_arvl_tm", "hsp_acct_id", "accommodation_abbr", "user_id", "row_insert_tsp", "row_updt_tsp", "insert_user_id", "updt_user_id", "msg_enqueued_tsp", "cncl_admsn_flg", "pregnancy_flg"],
  "epic_rltm.encntr_dx" : ["msg_src", "msg_tm","pat_enc_csn_id", "dx_icd_cd", "dx_nm", "dx_cd_typ", "row_insert_tsp","row_updt_tsp","insert_user_id","updt_user_id", "msg_enqueued_tsp"],
  "epic_rltm.encntr_er_complnt" : ["msg_src", "msg_tm", "pat_enc_csn_id", "er_complnt", "row_insert_tsp", "row_updt_tsp", "insert_user_id", "updt_user_id", "msg_enqueued_tsp"],
  "epic_rltm.encntr_visit_rsn" : ["msg_src", "msg_tm", "pat_enc_csn_id", "encntr_rsn", "row_insert_tsp", "row_updt_tsp", "insert_user_id", "updt_user_id", "msg_enqueued_tsp"],
  "epic_rltm.encntr_nte" : ["msg_src", "msg_tm", "pat_enc_csn_id", "nte_txt", "nte_typ", "row_insert_tsp", "row_updt_tsp", "insert_user_id", "updt_user_id", "msg_enqueued_tsp"]
}

# COMMAND ----------

from datetime import datetime,timezone
from pyspark.sql.functions import udf,lit
from pyspark.sql.types import *

def get_row_time():
  return datetime.now(timezone.utc)

add_row_insert_tsp = udf(get_row_time,TimestampType())

# COMMAND ----------

def add_row_insert_tsp_json(json_data):
  return {**json_data,**{"row_insert_tsp":str(datetime.now(timezone.utc))}}

# COMMAND ----------

def fix_timezone(df, timezone="US/Central",ignore_cols=[]):
  from pyspark.sql.functions import from_utc_timestamp
  timestamp_columns = [item[0] for item in df.dtypes if item[1] == 'timestamp' and item[0] not in ignore_cols]
  for column in timestamp_columns:
    df = df.withColumn(column,from_utc_timestamp(column,timezone))
  return df

# COMMAND ----------

from pyspark.sql.functions import *

def add_col_batch(batch,batch_reduced,trig_tags,col_name,val=None):
  
  window = Window.partitionBy("pat_enc_csn_id")
  
  if len(trig_tags) == 1:
    batch_filtered = batch.where(col("trigger_evnt")==trig_tags[0]) \
                          .withColumn("max_msg_nm",max(col("msg_nm")).over(window)) \
                          .where(col("msg_nm")== col("max_msg_nm")) \
                          .drop(col("max_msg_nm")) \
                          .withColumn("modifier_"+col_name,lit("modified")) \
                          .select(*["pat_enc_csn_id","modifier_"+col_name,col_name])
  if len(trig_tags) > 1:
    batch_filtered =   batch.where((col("trigger_evnt")==trig_tags[0]) | (col("trigger_evnt")==trig_tags[1])) \
                            .withColumn("max_msg_nm",max(col("msg_nm")).over(window)) \
                            .where(col("msg_nm") == col("max_msg_nm")) \
                            .drop(col("max_msg_nm")) \
                            .withColumn(col_name+"_tmp",when(col("trigger_evnt")==trig_tags[1],val).otherwise(batch[col_name])) \
                            .drop(col(col_name)) \
                            .withColumnRenamed(col_name+"_tmp",col_name)\
                            .withColumn("modifier_"+col_name,lit("modified"))\
                            .select(*["pat_enc_csn_id","modifier_"+col_name,col_name])
  
  batch_col_added    = batch_reduced.drop(col(col_name)) \
                                    .join(batch_filtered,["pat_enc_csn_id"],"left") \
                                    .na.fill("unmodified",["modifier_"+col_name])
  
  return batch_col_added

# COMMAND ----------

def add_adt_arrival_tm_batch(batch,batch_reduced):
  return add_col_batch(batch,batch_reduced,["A04"],"adt_arvl_tm").drop(*["hosp_admsn_tm","hosp_disch_tm","cncl_admsn_flg"])

# COMMAND ----------

def add_hosp_admsn_tm_batch(batch,batch_reduced):
  return add_col_batch(batch,batch_reduced,["A01"],"hosp_admsn_tm").select(*["pat_enc_csn_id","hosp_admsn_tm","modifier_hosp_admsn_tm"])

# COMMAND ----------

def add_hosp_disch_tm_batch(batch,batch_reduced):
  return add_col_batch(batch,batch_reduced,["A03","A13"],"hosp_disch_tm").select(*["pat_enc_csn_id","hosp_disch_tm","modifier_hosp_disch_tm"])

# COMMAND ----------

def add_cncl_admsn_flg_batch(batch,batch_reduced):
  return add_col_batch(batch,batch_reduced,["A01","A11"],"cncl_admsn_flg","True").select(*["pat_enc_csn_id","cncl_admsn_flg","modifier_cncl_admsn_flg"])

# COMMAND ----------

def add_all_cols_batch(batch,batch_reduced):
  return   add_adt_arrival_tm_batch(batch,batch_reduced) \
           .join(add_hosp_admsn_tm_batch(batch,batch_reduced),["pat_enc_csn_id"])\
           .join(add_hosp_disch_tm_batch(batch,batch_reduced),["pat_enc_csn_id"])\
           .join(add_cncl_admsn_flg_batch(batch,batch_reduced),["pat_enc_csn_id"])

# COMMAND ----------

from pyspark import Row
from pyspark.sql.functions import col,max
from pyspark.sql import Window
from pyspark.sql import functions as F

def get_transformed_batch(batch):
  
  window = Window.partitionBy("pat_enc_csn_id")
  batch_reduced = batch.withColumn("max_msg_nm",max(col("msg_nm")).over(window)) \
                  .where(col("msg_nm") >= col("max_msg_nm")) \
                  .drop(col("max_msg_nm"))
  batch_transformed_tm = add_all_cols_batch(batch,batch_reduced)
  return batch_transformed_tm
  

# COMMAND ----------

def get_transformed_child_batch(batch):
  
  window = Window.partitionBy("pat_enc_csn_id")
  batch_reduced = batch.withColumn("max_msg_tm",max(col("msg_tm")).over(window)) \
                  .where(col("msg_tm") == col("max_msg_tm")) \
                  .drop(col("max_msg_tm"))
  return batch_reduced

# COMMAND ----------

def get_update_condition(df,table_name,exclude_cols):
  columns = df.toDF().columns
  cond = {}
  for col in columns:
    if col not in exclude_cols:
      cond[col] = "{}.{}".format(table_name,col)
  cond["row_updt_tsp"] = "{}.row_insert_tsp".format(table_name)
  return cond

# COMMAND ----------

def get_insert_condition(df,table_name):
  columns = df.toDF().columns
  cond = {}
  for col in columns:
    cond[col] = "{}.{}".format(table_name,col)
  return cond

# COMMAND ----------

def read_from_synapse(table_name):
  return spark.read \
              .format("com.databricks.spark.sqldw") \
              .option("url", synapse_jdbc) \
              .option("tempdir", tempDir) \
              .option("useAzureMSI", "true") \
              .option("Query", "SELECT * FROM {}".format(table_name)) \
              .load()

def write_to_synapse(df,table_name,mode="append"):
  df.write \
    .format("com.databricks.spark.sqldw") \
    .mode(mode) \
    .option("url", synapse_jdbc) \
    .option("useAzureMSI", "true") \
    .option("dbtable", table_name) \
    .option("tempdir", tempDir) \
    .save()
  
def delete_from_synapse(from_delete_table,to_delete_table,delete_col):
  query = '''DELETE FROM {}
             WHERE {} IN
             (SELECT {} FROM {})'''.format(from_delete_table,delete_col,delete_col,to_delete_table)
  
  read_from_synapse(from_delete_table).write \
    .format("com.databricks.spark.sqldw") \
    .option("url", synapse_jdbc) \
    .option("useAzureMSI", "true") \
    .option("dbtable", from_delete_table) \
    .option("tempdir", tempDir) \
    .option("postActions",query)\
    .mode('overwrite') \
    .save()

def write_to_synapse_with_pre_delete(df, table_name, delete_col, delete_values, mode="append"):
  converted_values = [str(element) for element in delete_values]
  query = '''DELETE FROM {}
             WHERE {} IN ({})'''.format(table_name, delete_col, ",".join(converted_values))
  df.write \
    .format("com.databricks.spark.sqldw") \
    .mode(mode) \
    .option("url", synapse_jdbc) \
    .option("useAzureMSI", "true") \
    .option("dbtable", table_name) \
    .option("tempdir", tempDir) \
    .option("preActions",query)\
    .save()
  
  

# COMMAND ----------

from delta.tables import *

def captureChangeDataSynapse(batch,synapse_table_name):
  
  init_insert = False
  
  try:
    dtl_df = read_from_synapse(synapse_table_name)
    if dtl_df.count() == 0 :
      init_insert = True
  except:
    init_insert = True
  
  batch = batch.withColumn("cncl_admsn_flg",lit(None))
  batch_reduced = get_transformed_batch(batch.distinct())
  
  if init_insert:
      batch_reduced = batch_reduced.select(batch.columns)\
                                   .withColumn("row_updt_tsp",col("row_insert_tsp")) \
                                   .withColumn("updt_user_id",col("insert_user_id"))
      write_to_synapse(batch_reduced.select(select_sequence[synapse_table_name]),synapse_table_name)
    
  else:
      max_tm =     dtl_df.where(col("row_updt_tsp").isNotNull()) \
                         .select("row_updt_tsp") \
                         .agg(max("row_updt_tsp").alias("max_tm")) \
                         .collect()[0]["max_tm"]

      if max_tm is not None:
           batch_incremental = batch_reduced.where(col("row_insert_tsp") >= max_tm)
      else:
           batch_incremental = batch_reduced
      #print("-111111")
      batch_incremental = batch_incremental.withColumn("row_updt_tsp",col("row_insert_tsp")) \
                                           .withColumn("updt_user_id",col("insert_user_id"))
      #print("0000")

      txns = dtl_df.select("pat_enc_csn_id")\
                   .withColumn("if_exists",lit("Y"))\
                   .join(batch_incremental,["pat_enc_csn_id"],"right")

      inserts_df = fix_timezone(txns.where(col("if_exists").isNull())\
                       .drop("if_exists"))

      update_txns = txns.where(col("if_exists")=="Y")\
                        .drop("if_exists")

      synapse_delta = synapse_table_name.replace(".","_")
      #print("111111")
      dtl_df.join(batch_incremental.select("pat_enc_csn_id"),["pat_enc_csn_id"],"inner")\
            .write\
            .format("delta")\
            .mode("overwrite")\
            .saveAsTable(synapse_delta)

      dtl_df_delta = DeltaTable.forName(spark,synapse_delta)
      #print("22222")
      dtl_df_delta.alias("dtl") \
                     .merge(
                      update_txns.alias("hist"),
                      "dtl.pat_enc_csn_id = hist.pat_enc_csn_id") \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, \
                                     set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm","hosp_admsn_tm","hosp_disch_tm","cncl_admsn_flg"])) \
                      \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, \
                                        set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm","hosp_admsn_tm","cncl_admsn_flg"])) \
                      \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, 
                                        set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm","hosp_disch_tm","cncl_admsn_flg"])) \
                      \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, 
                                        set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp","hosp_admsn_tm","hosp_disch_tm","cncl_admsn_flg"])) \
                      \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, 
                                        set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm","cncl_admsn_flg"])) \
                      \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, 
                                        set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp","hosp_admsn_tm","cncl_admsn_flg"])) \
                      \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, 
                                        set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp","hosp_disch_tm","cncl_admsn_flg"])) \
                      \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, 
                                        set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp","cncl_admsn_flg"])) \
                      \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "modified" """, 
                                        set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm","hosp_admsn_tm","hosp_disch_tm"])) \
                      \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "modified" """, \
                                        set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm","hosp_admsn_tm"])) \
                      \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "modified" """, 
                                        set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm","hosp_disch_tm"])) \
                      \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "modified" """, 
                                        set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp","hosp_admsn_tm","hosp_disch_tm"])) \
                      \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "modified" """, 
                                        set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm"])) \
                      \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "modified" """, 
                                        set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp","hosp_admsn_tm"])) \
                      \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "modified" """, 
                                        set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp","hosp_disch_tm"])) \
                      \
                      .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "modified" """, 
                                        set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp"])) \
                      \
                      .whenNotMatchedInsert(values=get_insert_condition(dtl_df_delta,"hist")) \
                      \
                      .execute()

      #select_cols = batch.columns
      #print("3333")
      updates_df = fix_timezone(spark.sql("SELECT * FROM {}".format(synapse_delta)))#.select(*select_cols)

      delete_csns = [row["pat_enc_csn_id"] for row in updates_df.select("pat_enc_csn_id").distinct().collect()]
      
      if updates_df.count() > 0:
        #print(updates_df.select(select_sequence[synapse_table_name]).count())
        write_to_synapse_with_pre_delete(updates_df.select(select_sequence[synapse_table_name]), synapse_table_name, "pat_enc_csn_id", delete_csns)
      
      if inserts_df.count() > 0:
        #print("44444")
        if dtl_df.count() > 0:
          write_to_synapse(inserts_df.select(select_sequence[synapse_table_name]),synapse_table_name)
        else:
          write_to_synapse(inserts_df.select(select_sequence[synapse_table_name]),synapse_table_name, mode='overwrite')
                                    

# COMMAND ----------

def captureChangeDataChildSynapse(batch,add_cols,synapse_table_name):
  
  init_insert = False
  try:
    dtl_df = read_from_synapse(synapse_table_name)
    if dtl_df.count() == 0:
      init_insert = True
  except:
    init_insert = True
 
  batch_reduced = get_transformed_child_batch(batch.distinct())\
                  .withColumn("row_updt_tsp",col("row_insert_tsp")) \
                  .withColumn("updt_user_id",col("insert_user_id"))
  
  if init_insert:
      write_to_synapse(batch_reduced.withColumn("msg_tm", to_timestamp("msg_tm")).select(select_sequence[synapse_table_name]),synapse_table_name)
  else:
      txns = dtl_df.select("pat_enc_csn_id")\
                   .withColumn("if_exists",lit("Y"))\
                   .join(batch_reduced,["pat_enc_csn_id"],"right")

      inserts_df = txns.where(col("if_exists").isNull())\
                       .drop("if_exists")

      update_txns = txns.where(col("if_exists")=="Y")\
                        .drop("if_exists")

      synapse_delta = synapse_table_name.replace(".","_")

      dtl_df.join(batch_reduced.select("pat_enc_csn_id"),["pat_enc_csn_id"],"inner")\
            .write\
            .format("delta")\
            .mode("overwrite")\
            .saveAsTable(synapse_delta)\

      dtl_df_delta = DeltaTable.forName(spark,synapse_delta)
      
      batch_distinct_csns = [row["pat_enc_csn_id"] for row in batch_reduced.select("pat_enc_csn_id").distinct().collect()]
  
      merge_cond = "dtl.pat_enc_csn_id = hist.pat_enc_csn_id"
      for column in add_cols:
        merge_cond = merge_cond + " AND " + "dtl.{} = hist.{}".format(column,column)

      for csn in batch_distinct_csns:
        spark.sql("DELETE FROM {} WHERE pat_enc_csn_id = {}".format(synapse_delta,csn))

      dtl_df_delta.alias("dtl") \
                     .merge(
                      update_txns.alias("hist"),
                      "dtl.pat_enc_csn_id = hist.pat_enc_csn_id") \
                      .whenMatchedUpdate(set=get_update_condition(dtl_df_delta,"hist",["row_updt_tsp","row_insert_tsp"])) \
                      \
                      .whenNotMatchedInsert(values=get_insert_condition(dtl_df_delta,"hist")) \
                      \
                      .execute()

      updates_df = spark.sql("SELECT * FROM {}".format(synapse_delta))
      delete_txns = updates_df.select("pat_enc_csn_id")
      delete_table = "dbo.delete_{}_txns".format(synapse_delta)

      write_to_synapse(delete_txns,delete_table,"overwrite")
      delete_from_synapse(synapse_table_name,delete_table,"pat_enc_csn_id")

      write_to_synapse(updates_df.select(select_sequence[synapse_table_name]),synapse_table_name)
      
      if inserts_df.count() > 0:
        if dtl_df.count() > 0:
          write_to_synapse(inserts_df.select(select_sequence[synapse_table_name]),synapse_table_name)
        else:
          write_to_synapse(inserts_df.select(select_sequence[synapse_table_name]),synapse_table_name,mode='overwrite')
          

# COMMAND ----------

def captureChangeDataChildSynapseOptimizedDelete(batch,add_cols,synapse_table_name):
  
  init_insert = False
  try:
    dtl_df = read_from_synapse(synapse_table_name)
    if dtl_df.count() == 0:
      init_insert = True
  except:
    init_insert = True
 
  batch_reduced = fix_timezone(get_transformed_child_batch(batch.distinct())\
                  .withColumn("row_updt_tsp",col("row_insert_tsp")) \
                  .withColumn("updt_user_id",col("insert_user_id")))
  
  if init_insert:
      write_to_synapse(batch_reduced.withColumn("msg_tm", to_timestamp("msg_tm")).select(select_sequence[synapse_table_name]),synapse_table_name)
  else:
      delete_csns = [row["pat_enc_csn_id"] for row in batch_reduced.select("pat_enc_csn_id").distinct().collect()]
      if dtl_df.count() > 0:
        if len(delete_csns) > 0:
          write_to_synapse_with_pre_delete(batch_reduced.select(select_sequence[synapse_table_name]), synapse_table_name, "pat_enc_csn_id", delete_csns)
        if len(delete_csns) == 0 and batch_reduced.count() > 0:
          write_to_synapse(batch_reduced.select(select_sequence[synapse_table_name]), synapse_table_name)  
      else:
        write_to_synapse(batch_reduced.select(select_sequence[synapse_table_name]), synapse_table_name, mode="overwrite")
          

# COMMAND ----------

def captureChangeData(batch,batch_id,dtl_df,detail_table):
  
  df = spark.sql("SELECT * FROM {}".format(detail_table))
  max_tm =     df.where(col("row_updt_tsp").isNotNull()) \
               .select("row_updt_tsp") \
               .agg(max("row_updt_tsp").alias("max_tm")) \
               .collect()[0]["max_tm"]
  
  batch = batch.withColumn("cncl_admsn_flg",lit(None))
  batch_reduced = get_transformed_batch(batch.distinct())
  
  if max_tm is not None:
       batch_incremental = batch_reduced.where(col("row_insert_tsp") >= max_tm)
  else:
       batch_incremental = batch_reduced
   
  batch_incremental = batch_incremental.withColumn("row_updt_tsp",col("row_insert_tsp")) \
                                       .withColumn("updt_user_id",col("insert_user_id"))
                                       
  dtl_df.alias("dtl") \
                 .merge(
                  batch_incremental.alias("hist"),
                  "dtl.pat_enc_csn_id = hist.pat_enc_csn_id") \
                 .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, \
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm","hosp_admsn_tm","hosp_disch_tm","cncl_admsn_flg"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, \
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm","hosp_admsn_tm","cncl_admsn_flg"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, 
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm","hosp_disch_tm","cncl_admsn_flg"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, 
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp","hosp_admsn_tm","hosp_disch_tm","cncl_admsn_flg"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, 
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm","cncl_admsn_flg"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, 
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp","hosp_admsn_tm","cncl_admsn_flg"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, 
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp","hosp_disch_tm","cncl_admsn_flg"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "unmodified" """, 
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp","cncl_admsn_flg"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "modified" """, 
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm","hosp_admsn_tm","hosp_disch_tm"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "modified" """, \
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm","hosp_admsn_tm"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "modified" """, 
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm","hosp_disch_tm"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "modified" """, 
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp","hosp_admsn_tm","hosp_disch_tm"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "unmodified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "modified" """, 
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp","adt_arvl_tm"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "unmodified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "modified" """, 
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp","hosp_admsn_tm"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "unmodified" AND hist.modifier_cncl_admsn_flg = "modified" """, 
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp","hosp_disch_tm"])) \
                  \
                  .whenMatchedUpdate(""" hist.modifier_adt_arvl_tm = "modified" AND hist.modifier_hosp_admsn_tm = "modified" AND hist.modifier_hosp_disch_tm = "modified" AND hist.modifier_cncl_admsn_flg = "modified" """, 
                                     set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp"])) \
                  \
                  .whenNotMatchedInsert(values=get_insert_condition(dtl_df,"hist")) \
                  \
                  .execute()
      

# COMMAND ----------

def captureChangeDataChild(batch,batch_id,add_cols,dtl_df,detail_table):
  
  df = spark.sql("SELECT * FROM {}".format(detail_table))
  max_tm =     df.where(col("row_updt_tsp").isNotNull()) \
               .select("row_updt_tsp") \
               .agg(max("row_updt_tsp").alias("max_tm")) \
               .collect()[0]["max_tm"]
  
  batch_reduced = get_transformed_child_batch(batch.distinct())\
                  .withColumn("row_updt_tsp",col("row_insert_tsp")) \
                  .withColumn("updt_user_id",col("insert_user_id"))
  
  batch_distinct_csns = [row["pat_enc_csn_id"] for row in batch_reduced.select("pat_enc_csn_id").distinct().collect()]
  
  merge_cond = "dtl.pat_enc_csn_id = hist.pat_enc_csn_id"
  for column in add_cols:
    merge_cond = merge_cond + " AND " + "dtl.{} = hist.{}".format(column,column)
  
  for csn in batch_distinct_csns:
    spark.sql("DELETE FROM {} WHERE pat_enc_csn_id = {}".format(detail_table,csn))
  
  dtl_df.alias("dtl") \
                 .merge(
                  batch_reduced.alias("hist"),
                  merge_cond) \
                  .whenMatchedUpdate(set=get_update_condition(dtl_df,"hist",["row_updt_tsp","row_insert_tsp"])) \
                  \
                  .whenNotMatchedInsert(values=get_insert_condition(dtl_df,"hist")) \
                  \
                  .execute()

# COMMAND ----------

def captureChangeDataChildOptimizedDeletes(batch,batch_id,add_cols,dtl_df,detail_table):
  
  df = spark.sql("SELECT * FROM {}".format(detail_table))
  max_tm =     df.where(col("row_updt_tsp").isNotNull()) \
               .select("row_updt_tsp") \
               .agg(max("row_updt_tsp").alias("max_tm")) \
               .collect()[0]["max_tm"]
  
  batch_reduced = get_transformed_child_batch(batch.distinct())\
                  .withColumn("row_updt_tsp",col("row_insert_tsp")) \
                  .withColumn("updt_user_id",col("insert_user_id"))
  
  df_updated = df.join(batch_reduced.select("pat_enc_csn_id"),["pat_enc_csn_id"],"leftanti")\
                 .select(batch_reduced.columns)\
                 .union(batch_reduced)
  
  df_updated.write\
              .format("delta")\
              .mode("overwrite")\
              .saveAsTable(detail_table)


# COMMAND ----------

from delta.tables import *
def invoke_cdc_process(insert_table_name,detail_table_loc,detail_table_name,checkpoint_loc):
  
  dtl_df = DeltaTable.forPath(spark,detail_table_loc)
  
  spark.readStream \
     .format("delta") \
     .option("ignoreChanges", "true")\
     .table(insert_table_name) \
     .writeStream \
     .format("delta") \
     .option("checkpointLocation",checkpoint_loc) \
     .foreachBatch(lambda df,batch_id : captureChangeData(df,batch_id,dtl_df,detail_table_name)) \
     .start()
  

# COMMAND ----------

def invoke_cdc_child_process(insert_table_name,detail_table_loc,detail_table_name,checkpoint_loc):
  
  dtl_df = DeltaTable.forPath(spark,detail_table_loc)
  
  spark.readStream \
     .format("delta") \
     .option("ignoreChanges", "true")\
     .table(insert_table_name) \
     .writeStream \
     .format("delta")\
     .option("checkpointLocation",checkpoint_loc) \
     .foreachBatch(lambda df,batch_id : captureChangeDataChild(df,batch_id,dtl_df,detail_table_name)) \
     .start()
  

# COMMAND ----------

from delta.tables import *
def invoke_cdc_subscriber(batch_hist_df,batch_id,detail_table_loc,detail_table_name):
  dtl_df = DeltaTable.forPath(spark,detail_table_loc)
  captureChangeData(batch_hist_df,batch_id,dtl_df,detail_table_name)
  

# COMMAND ----------

def invoke_cdc_child_subscriber(batch_hist_df,batch_id,add_cols,detail_table_loc,detail_table_name):
  dtl_df = DeltaTable.forPath(spark,detail_table_loc)
  captureChangeDataChildOptimizedDeletes(batch_hist_df,batch_id,add_cols,dtl_df,detail_table_name)