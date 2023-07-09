# Databricks notebook source
# MAGIC %run ../../utils/utilities

# COMMAND ----------

# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

# MAGIC %run ../../../../etl/data_engineering/commons/utilities

# COMMAND ----------

select_sequence = {
  "epic_rltm.adt_hist" : [ "msg_typ", "msg_src", "trigger_evnt", "msg_tm", "msg_nm", "bod_id", "pat_mrn_id", "pat_enc_csn_id", "birth_dt",  "death_dt", "pat_class_abbr" ,"hosp_admsn_tm", "hosp_disch_tm", "dept_abbr", "loc_abbr", "room_nm", "bed_label", "bed_stat", "sex_abbr", "means_of_arrv_abbr", "acuity_level_abbr", "ed_dsptn_abbr", "disch_dsptn_abbr", "adt_arvl_tm", "hsp_acct_id", "accommodation_abbr", "user_id", "row_insert_tsp", "row_updt_tsp", "insert_user_id", "updt_user_id", "msg_enqueued_tsp", "pregnancy_flg"],
  "epic_rltm.encntr_dx_hist" : ["msg_src", "msg_tm","pat_enc_csn_id", "dx_icd_cd", "dx_nm", "dx_cd_typ", "row_insert_tsp","row_updt_tsp","insert_user_id","updt_user_id", "msg_enqueued_tsp"],
  "epic_rltm.encntr_er_complnt_hist" : ["msg_src", "msg_tm", "pat_enc_csn_id", "er_complnt", "row_insert_tsp", "row_updt_tsp", "insert_user_id", "updt_user_id", "msg_enqueued_tsp"],
  "epic_rltm.encntr_visit_rsn_hist" : ["msg_src", "msg_tm", "pat_enc_csn_id", "encntr_rsn", "row_insert_tsp", "row_updt_tsp", "insert_user_id", "updt_user_id", "msg_enqueued_tsp"],
  "epic_rltm.encntr_nte_hist" : ["msg_src", "msg_tm", "pat_enc_csn_id", "nte_txt", "nte_typ", "row_insert_tsp", "row_updt_tsp", "insert_user_id", "updt_user_id", "msg_enqueued_tsp"]
}

# COMMAND ----------

adt_hist_columns = ['msg_typ','msg_src','trigger_evnt','msg_tm','msg_nm','bod_id','pat_mrn_id','pat_enc_csn_id','birth_dt','death_dt','pat_class_abbr',
                    'hosp_admsn_tm','hosp_disch_tm','dept_abbr','loc_abbr','room_nm','bed_label','bed_stat','sex_abbr','means_of_arrv_abbr',
                    'acuity_level_abbr','ed_dsptn_abbr','disch_dsptn_abbr','adt_arvl_tm','hsp_acct_id','accommodation_abbr','user_id',
                    'row_insert_tsp','row_updt_tsp','insert_user_id','updt_user_id', 'msg_enqueued_tsp', 'pregnancy_flg']

encntr_dx_hist_columns = ["msg_src","msg_tm","pat_enc_csn_id","dx_icd_cd","dx_nm","dx_cd_typ","row_insert_tsp","row_updt_tsp","insert_user_id","updt_user_id","msg_enqueued_tsp"]
encntr_er_complnt_hist_columns = ["msg_src","msg_tm","pat_enc_csn_id","er_complnt","row_insert_tsp","row_updt_tsp","insert_user_id","updt_user_id","msg_enqueued_tsp"]

encntr_visit_rsn_columns=["msg_src","msg_tm","pat_enc_csn_id","encntr_rsn","row_insert_tsp","row_updt_tsp","insert_user_id","updt_user_id","msg_enqueued_tsp"]
encntr_nte_columns=["msg_src","msg_tm","pat_enc_csn_id","nte_txt","nte_typ","row_insert_tsp","row_updt_tsp","insert_user_id","updt_user_id","msg_enqueued_tsp"]

# COMMAND ----------

def read_from_synapse(table_name):
  return spark.read \
              .format("com.databricks.spark.sqldw") \
              .option("url", synapse_jdbc) \
              .option("tempdir", tempDir) \
              .option("useAzureMSI", "true") \
              .option("Query", "SELECT * FROM {}".format(table_name)) \
              .load()

# COMMAND ----------

from delta.tables import *

def merge_into(batch,hist_table_loc):
  hist_df = DeltaTable.forPath(spark,hist_table_loc)
  hist_df.alias("hist")\
         .merge(batch.alias("batch"),
         "hist.msg_nm = batch.msg_nm")\
          .whenMatchedUpdateAll()\
          .whenNotMatchedInsertAll()

def merge_into_child(batch,hist_table):
  df = spark.sql("SELECT * FROM {}".format(hist_table))
  
  df_updated = batch.join(df.select("msg_tm"),["msg_tm"],"left_anti")\
                 .select(batch.columns)
                 
  df_updated.write\
              .format("delta")\
              .mode("append")\
              .saveAsTable(hist_table)

# COMMAND ----------

def merge_into_synapse(batch,synapse_table_name,merge_col):
  
  init_insert = False
  try:
    hist_df = read_from_synapse(synapse_table_name)
    if hist_df.count() == 0:
      init_insert = True
  except:
    init_insert = True
  
  if init_insert:
      write_to_synapse(batch.select(select_sequence[synapse_table_name]),synapse_table_name,mode='append')
  else:
      # txns = hist_df.select(merge_col)\
      #              .withColumn("if_exists",lit("Y"))\
      #              .join(batch,[merge_col],"right")

      # inserts_df = txns.where(col("if_exists").isNull())\
      #                  .drop("if_exists")

      # duplicate_txns = txns.where(col("if_exists")=="Y")\
      #                      .drop("if_exists")

      inserts_df = batch.join(hist_df.select(merge_col),[merge_col],"left_anti")\
                 .select(select_sequence[synapse_table_name])
      
      if inserts_df.count() > 0:
        # if hist_df.count() > 0:
        #   write_to_synapse(inserts_df.select(select_sequence[synapse_table_name]),synapse_table_name,mode='append')
        # else:
        #   write_to_synapse(inserts_df.select(select_sequence[synapse_table_name]),synapse_table_name,mode='append')
        write_to_synapse(inserts_df.select(select_sequence[synapse_table_name]),synapse_table_name,mode='append')
          

# COMMAND ----------

def create_adt_hist(adt_hist_rows, adt_hist_schema_src):
 
  return spark.createDataFrame(adt_hist_rows, adt_hist_schema_src).withColumn("birth_dt", to_timestamp("birth_dt")) \
              .withColumn("death_dt", to_timestamp("death_dt")) \
              .withColumn("msg_tm", to_timestamp("msg_tm")) \
              .withColumn("hosp_admsn_tm", to_timestamp("hosp_admsn_tm")) \
              .withColumn("hosp_disch_tm", to_timestamp("hosp_disch_tm")) \
              .withColumn("adt_arvl_tm", to_timestamp("adt_arvl_tm")) \
              .withColumn("msg_enqueued_tsp", to_timestamp("msg_enqueued_tsp")) \
              .withColumn("row_insert_tsp", lit(datetime.now())) \
              .withColumn("row_updt_tsp",col("row_insert_tsp"))\
              .withColumn("updt_user_id",col("insert_user_id"))\
              .withColumn("pat_enc_csn_id", col("pat_enc_csn_id").cast(DecimalType(18, 0))) \
              .withColumn("hsp_acct_id", col("hsp_acct_id").cast(DecimalType(18, 0))) \
              .select(adt_hist_columns) \
              .na.drop(subset=["msg_src","pat_enc_csn_id"]) 
     
  
def create_encntr_dx_hist(encntr_dx_hist_rows, encntr_dx_hist_schema_src):

  return spark.createDataFrame(encntr_dx_hist_rows, encntr_dx_hist_schema_src) \
              .withColumn("msg_tm", to_timestamp("msg_tm"))\
              .withColumn("row_insert_tsp", lit(datetime.now())) \
              .withColumn("msg_enqueued_tsp", to_timestamp("msg_enqueued_tsp")) \
              .withColumn("row_updt_tsp",col("row_insert_tsp"))\
              .withColumn("updt_user_id",col("insert_user_id"))\
              .withColumn("pat_enc_csn_id", col("pat_enc_csn_id").cast(DecimalType(18, 0))) \
              .select(encntr_dx_hist_columns) \
              .na.drop(subset=["msg_src","pat_enc_csn_id"]) 
    
  
def create_encntr_er_complnt_hist(encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src):

  return spark.createDataFrame(encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src) \
              .withColumn("msg_tm", to_timestamp("msg_tm"))\
              .withColumn("row_insert_tsp", lit(datetime.now())) \
              .withColumn("row_updt_tsp",col("row_insert_tsp"))\
              .withColumn("updt_user_id",col("insert_user_id"))\
              .withColumn("msg_enqueued_tsp", to_timestamp("msg_enqueued_tsp")) \
              .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast(DecimalType(18,0))) \
              .select(encntr_er_complnt_hist_columns) \
              .na.drop(subset=["msg_src","pat_enc_csn_id"])
      
  
def create_encntr_visit_rsn_hist(encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src):

  return spark.createDataFrame(encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src) \
              .withColumn("msg_tm", to_timestamp("msg_tm"))\
              .withColumn("row_insert_tsp", lit(datetime.now())) \
              .withColumn("row_updt_tsp",col("row_insert_tsp"))\
              .withColumn("updt_user_id",col("insert_user_id"))\
              .withColumn("msg_enqueued_tsp", to_timestamp("msg_enqueued_tsp")) \
              .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast(DecimalType(18,0))) \
              .select(encntr_visit_rsn_columns) \
              .na.drop(subset=["msg_src","pat_enc_csn_id"]) \
              
  
def create_encntr_nte_hist(encntr_nte_hist_rows, encntr_nte_hist_schema_src):

  return spark.createDataFrame(encntr_nte_hist_rows, encntr_nte_hist_schema_src) \
              .withColumn("msg_tm", to_timestamp("msg_tm"))\
              .withColumn("row_insert_tsp", lit(datetime.now())) \
              .withColumn("row_updt_tsp",col("row_insert_tsp"))\
              .withColumn("updt_user_id",col("insert_user_id"))\
              .withColumn("msg_enqueued_tsp", to_timestamp("msg_enqueued_tsp")) \
              .withColumn("pat_enc_csn_id",col("pat_enc_csn_id").cast(DecimalType(18,0))) \
              .select(encntr_nte_columns) \
              .na.drop(subset=["msg_src","pat_enc_csn_id"]) 
      
      
  

# COMMAND ----------

def insert_into_adt_hist(adt_hist_rows, adt_hist_schema_src, adt_hist_table):
  
      create_adt_hist(adt_hist_rows, adt_hist_schema_src)\
     .write.format("delta").mode("append").insertInto(adt_hist_table)
  
def insert_into_encntr_dx_hist(encntr_dx_hist_rows, encntr_dx_hist_schema_src, encntr_dx_hist_table):

     create_encntr_dx_hist(encntr_dx_hist_rows, encntr_dx_hist_schema_src)\
    .write.format("delta").mode("append").insertInto(encntr_dx_hist_table) 
  
def insert_into_encntr_er_complnt_hist(encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src, encntr_er_complnt_hist_table):

      create_encntr_er_complnt_hist(encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src)\
      .write.format("delta").mode("append").insertInto(encntr_er_complnt_hist_table)
  
def insert_into_encntr_visit_rsn_hist(encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src, encntr_visit_rsn_hist_table):

      create_encntr_visit_rsn_hist(encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src)\
      .write.format("delta").mode("append").insertInto(encntr_visit_rsn_hist_table)
  
def insert_into_encntr_nte_hist(encntr_nte_hist_rows, encntr_nte_hist_schema_src, encntr_nte_hist_table):

      create_encntr_nte_hist(encntr_nte_hist_rows, encntr_nte_hist_schema_src)\
      .write.format("delta").mode("append").insertInto(encntr_nte_hist_table)
      
  

# COMMAND ----------

def insert_into_adt_hist_synapse(adt_hist_rows, adt_hist_schema_src, adt_hist_table):
  
      batch = create_adt_hist(adt_hist_rows, adt_hist_schema_src)
      write_to_synapse(batch, adt_hist_table,mode='append') 
  
def insert_into_encntr_dx_hist_synapse(encntr_dx_hist_rows, encntr_dx_hist_schema_src, encntr_dx_hist_table):

    batch = create_encntr_dx_hist(encntr_dx_hist_rows, encntr_dx_hist_schema_src)
    write_to_synapse(batch, encntr_dx_hist_table,mode='append') 
  
def insert_into_encntr_er_complnt_hist_synapse(encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src, encntr_er_complnt_hist_table):

      batch = create_encntr_er_complnt_hist(encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src)
      write_to_synapse(batch, encntr_er_complnt_hist_table,mode='append') 
  
def insert_into_encntr_visit_rsn_hist_synapse(encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src, encntr_visit_rsn_hist_table):

      batch = create_encntr_visit_rsn_hist(encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src)
      write_to_synapse(batch, encntr_visit_rsn_hist_table,mode='append') 
  
def insert_into_encntr_nte_hist_synapse(encntr_nte_hist_rows, encntr_nte_hist_schema_src, encntr_nte_hist_table):

      batch = create_encntr_nte_hist(encntr_nte_hist_rows, encntr_nte_hist_schema_src)
      write_to_synapse(batch, encntr_nte_hist_table,mode='append') 
      
  

# COMMAND ----------

def merge_into_adt_hist(adt_hist_rows, adt_hist_schema_src, adt_hist_table, adt_hist_table_loc):
  
      batch = create_adt_hist(adt_hist_rows, adt_hist_schema_src)
      merge_into(batch,adt_hist_table_loc)
  
def merge_into_encntr_dx_hist(encntr_dx_hist_rows, encntr_dx_hist_schema_src, encntr_dx_hist_table, encntr_dx_hist_table_loc):

     batch = create_encntr_dx_hist(encntr_dx_hist_rows, encntr_dx_hist_schema_src)
     merge_into_child(batch,encntr_dx_hist_table) 
  
def merge_into_encntr_er_complnt_hist(encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src, encntr_er_complnt_hist_table, encntr_er_complnt_hist_table_loc):

      batch = create_encntr_er_complnt_hist(encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src)
      merge_into_child(batch,encntr_er_complnt_hist_table)
  
def merge_into_encntr_visit_rsn_hist(encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src, encntr_visit_rsn_hist_table, encntr_visit_rsn_hist_table_loc):

      batch = create_encntr_visit_rsn_hist(encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src)
      merge_into_child(batch,encntr_visit_rsn_hist_table)
  
def merge_into_encntr_nte_hist(encntr_nte_hist_rows, encntr_nte_hist_schema_src, encntr_nte_hist_table, encntr_nte_hist_table_loc):

      batch = create_encntr_nte_hist(encntr_nte_hist_rows, encntr_nte_hist_schema_src)
      merge_into_child(batch,encntr_nte_hist_table)

# COMMAND ----------

def merge_into_adt_hist_synapse(adt_hist_rows, adt_hist_schema_src, adt_hist_table, adt_hist_merge_col):
  
      batch = create_adt_hist(adt_hist_rows, adt_hist_schema_src)
      merge_into_synapse(batch,adt_hist_table,adt_hist_merge_col)
  
def merge_into_encntr_dx_hist_synapse(encntr_dx_hist_rows, encntr_dx_hist_schema_src, encntr_dx_hist_table, encntr_dx_hist_merge_col):

     batch = create_encntr_dx_hist(encntr_dx_hist_rows, encntr_dx_hist_schema_src)
     merge_into_synapse(batch,encntr_dx_hist_table,encntr_dx_hist_merge_col) 
  
def merge_into_encntr_er_complnt_hist_synapse(encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src, encntr_er_complnt_hist_table, encntr_er_complnt_hist_merge_col):

      batch = create_encntr_er_complnt_hist(encntr_er_complnt_hist_rows, encntr_er_complnt_hist_schema_src)
      merge_into_synapse(batch,encntr_er_complnt_hist_table,encntr_er_complnt_hist_merge_col)
  
def merge_into_encntr_visit_rsn_hist_synapse(encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src, encntr_visit_rsn_hist_table, encntr_visit_rsn_hist_merge_col):

      batch = create_encntr_visit_rsn_hist(encntr_visit_rsn_hist_rows, encntr_visit_rsn_hist_schema_src)
      merge_into_synapse(batch,encntr_visit_rsn_hist_table,encntr_visit_rsn_hist_merge_col)
  
def merge_into_encntr_nte_hist_synapse(encntr_nte_hist_rows, encntr_nte_hist_schema_src, encntr_nte_hist_table, encntr_nte_hist_merge_col):

      batch = create_encntr_nte_hist(encntr_nte_hist_rows, encntr_nte_hist_schema_src)
      merge_into_synapse(batch,encntr_nte_hist_table,encntr_nte_hist_merge_col)
      
  

# COMMAND ----------

