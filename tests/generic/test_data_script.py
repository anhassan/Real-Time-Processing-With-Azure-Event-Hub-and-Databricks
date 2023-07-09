# Databricks notebook source
def get_adt_hist_test_data():
  query_statement =  '''select distinct
                        pat.pat_mrn_id          as pat_mrn_id,               
                        peh.pat_enc_csn_id      as pat_enc_csn_id,    
                        pat.birth_date          as birth_date,
                        pat.death_date          as death_date,
                        upper(zc_class.abbr)    as pat_class_abbr,	
                        peh.hosp_admsn_time     as hosp_admsn_time,	
                        peh.hosp_disch_time     as hosp_disch_time,
                        dep_loc.department_abbr as department_abbr,
                        dep_loc.loc_abbr        as loc_abbr,
                        rom.room_name 			as room_nm,
                        bed.bed_label           as bed_label,
                        zc_bed.name 			as bed_status,
                        zc_sex.abbr 			as sex_abbr,
                        zc_arriv.name 			as means_of_arrv_abbr,
                        substr(zc_acuity.abbr,3)as acuity_level_abbr, 
                        zc_ed_disp.abbr 		as ed_disposition_abbr,
                        zc_disp.abbr            as disch_disp_abbr,
                        peh.adt_arrival_time    as adt_arrival_time,
                        peh.hsp_account_id,
                        c_curr.effective_time,  
                        c_curr.user_id,
                        zc_accomm.abbr          as accommodation_abbr,
                        row_number() over (partition by peh.pat_enc_csn_id,pat.pat_mrn_id  order by  c_curr.seq_num_in_enc desc) as row_num,
                        bed.bed_status_c,
                        bed.census_inclusn_yn,
                        peh.disch_disp_c,
                        peh.acuity_level_c,
                        peh.means_of_arrv_c,
                        peh.ed_disposition_c,
                        c_curr.accommodation_c
                        from            clarity.pat_enc_hsp        peh
                        inner join      clarity.clarity_adt        c_curr   on peh.pat_enc_csn_id   = c_curr.pat_enc_csn_id
                                                                          and CAST(c_curr.effective_time as date) >= to_date('2021-07-01','yyyy-MM-dd')   
                                                                          and CAST(c_curr.effective_time as date) <=  (current_date -1)
                                                                          and c_curr.event_type_c in (1,2,3,5,6)   ---Admission/ Discharge/Transfer In/Patient Update/Census
                                                                          and c_curr.event_subtype_c <> 2          --- Filtering out Canceled Events
                        inner join      clarity.patient             pat         on pat.pat_id                   = peh.pat_id
                        inner join      clarity.patient_3           p3          on peh.pat_id                   = p3.pat_id and p3.is_test_pat_yn <> 'Y'    --   Exclude Test patients 
                        left outer join clarity.zc_pat_class        zc_class    on peh.adt_pat_class_c          = zc_class.adt_pat_class_c
                        inner join      bi_clarity.mv_dep_loc_sa    dep_loc     on c_curr.department_id         = dep_loc.department_id
                        inner join      clarity.clarity_rom         rom         on c_curr.room_csn_id           = rom.room_csn_id 
                        inner join      clarity.clarity_bed         bed         on bed.bed_csn_id               = c_curr.bed_csn_id   
                        left outer join clarity.zc_disch_disp       zc_disp     on peh.disch_disp_c             = zc_disp.disch_disp_c 
                        left outer join clarity.zc_acuity_level	    zc_acuity	on zc_acuity.acuity_level_c	    = peh.acuity_level_c	
                        left outer join clarity.zc_sex 			    zc_sex      on zc_sex.rcpt_mem_sex_c	    = pat.sex_c
                        left outer join clarity.zc_arriv_means      zc_arriv    on zc_arriv.means_of_arrv_c     = peh.means_of_arrv_c
                        left outer join clarity.zc_ed_disposition   zc_ed_disp  on zc_ed_disp.ed_disposition_c  = peh.ed_disposition_c
                        left outer join clarity.zc_bed_status		zc_bed      on zc_bed.bed_status_c          = bed.bed_status_c
                        left outer join clarity.zc_accommodation	zc_accomm   on  zc_accomm.accommodation_c   = c_curr.accommodation_c
                        where  (CAST(peh.hosp_admsn_time as date) >= to_date('2021-07-01','yyyy-MM-dd') 
                                  and (peh.hosp_disch_time is null
                                        or   CAST(peh.hosp_disch_time as date) >= (current_date -2)
                                       )
                                )
                        and dep_loc.sa_rpt_grp_six_name like '%MERCY%'
                        and dep_loc.loc_id   not in (120230,120112)  
                        and peh.admit_conf_stat_c <> 3               
                        and pat.death_date is null;
                        '''
  
  filter_cols = ["pat_mrn_id","pat_enc_csn_id","birth_date","death_date","pat_class_abbr",	
                 "hosp_admsn_time","hosp_disch_time","department_abbr","loc_abbr","room_nm","bed_label",
                 "bed_status","sex_abbr","means_of_arrv_abbr","acuity_level_abbr", "ed_disposition_abbr",
                 "disch_disp_abbr","adt_arrival_time","hsp_account_id","accommodation_abbr","user_id"]
  
  df = spark.sql(query_statement)\
            .where(col("row_num")==1)\
            .select(*filter_cols)\
            .distinct()
  
  return df

# COMMAND ----------

def get_encntr_dx_hist_test_data(adt_hist):
  clarity_pat_enc_dx = spark.sql("SELECT * FROM clarity.pat_enc_dx")
  clarity_edg_current_icd10 = spark.sql("SELECT * FROM clarity.edg_current_icd10")
  clarity_clarity_edg = spark.sql("SELECT * FROM clarity.clarity_edg")
  
  filter_cols = ["pat_enc_csn_id","code","dx_name","dx_code_type"]
  
  return adt_hist.join(clarity_pat_enc_dx,["pat_enc_csn_id"])\
                 .join(clarity_edg_current_icd10,["dx_id"])\
                 .join(clarity_clarity_edg,["dx_id"])\
                 .withColumn("dx_code_type",lit("ICD-10-CM"))\
                 .select(*filter_cols)\
                 .withColumnRenamed("code","dx_icd_cd")

# COMMAND ----------

def get_encntr_er_complnt_hist_test_data(adt_hist):
  clarity_pat_enc_er_complnt = spark.sql("SELECT * FROM clarity.pat_enc_er_complnt er_com")
  
  filter_cols = ["pat_enc_csn_id","er_complaint"]
  
  return adt_hist.join(clarity_pat_enc_er_complnt,["pat_enc_csn_id"])\
                 .select(*filter_cols)\
                 .distinct()

# COMMAND ----------

def get_encntr_visit_rsn_hist_test_data(adt_hist):
  clarity_pat_enc_rsn_visit = spark.sql("SELECT * FROM clarity.pat_enc_rsn_visit")
  clarity_cl_rsn_for_visit = spark.sql("SELECT * FROM  clarity.cl_rsn_for_visit")\
                                  .withColumnRenamed("reason_visit_id","enc_reason_id")
  
  filter_cols = ["pat_enc_csn_id","reason_visit_name"]
  
  return adt_hist.join(clarity_pat_enc_rsn_visit,["pat_enc_csn_id"])\
                 .join(clarity_cl_rsn_for_visit,["enc_reason_id"])\
                 .select(*filter_cols)\
                 .withColumnRenamed("reason_visit_name","encntr_rsn")\
                 .distinct()

# COMMAND ----------

