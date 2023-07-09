# Databricks notebook source
#This notebook is used for one time laod of ADT History and Detail Tables

# COMMAND ----------

# STEP1 - Schedule Postman to stop the job at 11:50pm - epic_rltm_adt_hl7_srvc_bus_mult_cnsmr(queue to topic)
# STEP2 - On the day of one time load, check Service bus queue(adt_from_epic) to delete any messages from prior day
# STEP3 - Stop the jobs for all 4 subscribers in Databricks
# STEP4 - Run all the steps in ddl's to drop and recreate the history and detail tables
# STEP5 - Follow cmd2 steps
# STEP6 - Follow cmd14 steps
# STEP7 - Start all 5 jobs in Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ##### STEP4 - Run Below Insert statements to load data into Delta tables

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into epic_rltm.adt_dtl
# MAGIC with adt_dtl as (
# MAGIC select distinct
# MAGIC 'ETL'                   as msg_typ,
# MAGIC 'STL'                   as msg_src,
# MAGIC 'ETL'                   as trigger_evnt,  
# MAGIC current_timestamp       as msg_tm,
# MAGIC 'ETL'                   as msg_nm,
# MAGIC 'ETL'                   as bod_id,
# MAGIC pat.pat_mrn_id          as pat_mrn_id,               
# MAGIC peh.pat_enc_csn_id      as pat_enc_csn_id,    
# MAGIC pat.birth_date          as birth_dt,
# MAGIC pat.death_date          as death_dt,
# MAGIC upper(zc_class.abbr)    as pat_class_abbr,	
# MAGIC peh.hosp_admsn_time     as hosp_admsn_tm,	
# MAGIC peh.hosp_disch_time     as hosp_disch_tm,
# MAGIC dep_loc.department_abbr as dept_abbr,
# MAGIC dep_loc.loc_abbr        as loc_abbr,
# MAGIC rom.room_name 			as room_nm,
# MAGIC bed.bed_label           as bed_label,
# MAGIC zc_bed.name 			as bed_stat,
# MAGIC zc_sex.abbr 			as sex_abbr,
# MAGIC zc_arriv.name 			as means_of_arrv_abbr,
# MAGIC substr(zc_acuity.abbr,3)as acuity_level_abbr, 
# MAGIC zc_ed_disp.abbr 		as ed_dsptn_abbr,
# MAGIC zc_disp.abbr            as disch_dsptn_abbr,
# MAGIC peh.adt_arrival_time    as adt_arvl_tm,
# MAGIC peh.hsp_account_id      as hsp_acct_id,
# MAGIC c_curr.effective_time,  
# MAGIC c_curr.user_id          as user_id,
# MAGIC zc_accomm.abbr          as accommodation_abbr,
# MAGIC  --- TO DETERMINE THE FINAL DEPARTMENT.
# MAGIC row_number() over (partition by peh.pat_enc_csn_id,pat.pat_mrn_id  order by  c_curr.seq_num_in_enc desc) as row_num,
# MAGIC bed.bed_status_c,
# MAGIC bed.census_inclusn_yn,
# MAGIC peh.disch_disp_c,
# MAGIC peh.acuity_level_c,
# MAGIC peh.means_of_arrv_c,
# MAGIC peh.ed_disposition_c,
# MAGIC c_curr.accommodation_c,
# MAGIC case when obgyn_stat_c = 4 then 'Y' else null end as pregnancy_flg
# MAGIC from            clarity.pat_enc_hsp        peh
# MAGIC inner join      clarity.clarity_adt        c_curr   on peh.pat_enc_csn_id   = c_curr.pat_enc_csn_id
# MAGIC                                                   and CAST(c_curr.effective_time as date) >= to_date('2021-07-01','yyyy-MM-dd')   
# MAGIC                                                   and CAST(c_curr.effective_time as date) <=  (current_date -1)
# MAGIC                                                   and c_curr.event_type_c in (1,2,3,5,6)   ---Admission/ Discharge/Transfer In/Patient Update/Census
# MAGIC                                                   and c_curr.event_subtype_c <> 2          --- Filtering out Canceled Events
# MAGIC inner join      clarity.patient             pat         on pat.pat_id                   = peh.pat_id
# MAGIC inner join      clarity.patient_3           p3          on peh.pat_id                   = p3.pat_id and p3.is_test_pat_yn <> 'Y'    --   Exclude Test patients 
# MAGIC left outer join clarity.zc_pat_class        zc_class    on peh.adt_pat_class_c          = zc_class.adt_pat_class_c
# MAGIC inner join      bi_clarity.mv_dep_loc_sa    dep_loc     on c_curr.department_id         = dep_loc.department_id
# MAGIC inner join      clarity.clarity_rom         rom         on c_curr.room_csn_id           = rom.room_csn_id 
# MAGIC inner join      clarity.clarity_bed         bed         on bed.bed_csn_id               = c_curr.bed_csn_id   
# MAGIC left outer join clarity.zc_disch_disp       zc_disp     on peh.disch_disp_c             = zc_disp.disch_disp_c 
# MAGIC left outer join clarity.zc_acuity_level	    zc_acuity	on zc_acuity.acuity_level_c	    = peh.acuity_level_c	
# MAGIC left outer join clarity.zc_sex 			    zc_sex      on zc_sex.rcpt_mem_sex_c	    = pat.sex_c
# MAGIC left outer join clarity.zc_arriv_means      zc_arriv    on zc_arriv.means_of_arrv_c     = peh.means_of_arrv_c
# MAGIC left outer join clarity.zc_ed_disposition   zc_ed_disp  on zc_ed_disp.ed_disposition_c  = peh.ed_disposition_c
# MAGIC left outer join clarity.zc_bed_status		zc_bed      on zc_bed.bed_status_c          = bed.bed_status_c
# MAGIC left outer join clarity.zc_accommodation	zc_accomm   on  zc_accomm.accommodation_c   = c_curr.accommodation_c
# MAGIC left outer join (select ob_app_date.pat_id,
# MAGIC                             ob_app_date.pat_enc_csn_id,
# MAGIC                             ob3.obgyn_stat_c,
# MAGIC                             ob3.app_inst_utc_dttm,
# MAGIC                             ob_app_date.obgyn_eff_dttm
# MAGIC                           from
# MAGIC                               --Find the latest 'OB Status applied date' for each encounter
# MAGIC                               (
# MAGIC                               select pe.pat_id, pe.pat_enc_csn_id, pe.obgyn_eff_dttm, max(ob.app_inst_utc_dttm) app_inst_utc_dttm
# MAGIC                               from clarity.pat_enc_5 pe inner join clarity.obgyn_stat ob on (pe.pat_id=ob.pat_id and pe.obgyn_eff_dttm >= ob.app_inst_utc_dttm)
# MAGIC                               group by pe.pat_id, pe.pat_enc_csn_id,pe.obgyn_eff_dttm
# MAGIC                               ) ob_app_date
# MAGIC                           left outer join
# MAGIC                               --Find the value with the highest line number for each 'OB Status applied date' for each patient
# MAGIC                               (
# MAGIC                               select ob3.pat_id, ob3.app_inst_utc_dttm, ob3.obgyn_stat_c
# MAGIC                               from clarity.obgyn_stat ob3
# MAGIC                               inner join (
# MAGIC                                   select pat_id, max(line) line
# MAGIC                                   from clarity.obgyn_stat ob2
# MAGIC                                   group by pat_id, app_inst_utc_dttm
# MAGIC                                           ) maxlines
# MAGIC                               on ob3.pat_id = maxlines.pat_id and ob3.line = maxlines.line
# MAGIC                               ) ob3
# MAGIC                           on ob_app_date.pat_id = ob3.pat_id and ob_app_date.app_inst_utc_dttm = ob3.app_inst_utc_dttm
# MAGIC                           where ob3.obgyn_stat_c = 4 -- Pregnancy
# MAGIC                           )  Preg on preg.pat_enc_csn_id = c_curr.pat_enc_csn_id
# MAGIC where  (CAST(peh.hosp_admsn_time as date) >= to_date('2021-07-01','yyyy-MM-dd') 
# MAGIC           and (peh.hosp_disch_time is null                                      --- Discharge time not present
# MAGIC                 or   CAST(peh.hosp_disch_time as date) >= (current_date -2)      --- Discharge's in the last two days.
# MAGIC                )
# MAGIC         )  
# MAGIC and dep_loc.sa_rpt_grp_six_name like '%MERCY%' --- Limiting only to only 'Mercy' Service Area's
# MAGIC and dep_loc.loc_id   not in (120230,120112)    --- Filtering out Hot Springs And Mercy Hospital Independence
# MAGIC and peh.admit_conf_stat_c <> 3                 --- Filtering out cancelled admissions
# MAGIC and pat.death_date is null                     --- Filtering out any patients that had death date populated
# MAGIC )
# MAGIC     
# MAGIC select distinct
# MAGIC msg_typ,
# MAGIC msg_src,
# MAGIC trigger_evnt,  
# MAGIC msg_tm,
# MAGIC msg_nm,
# MAGIC bod_id,
# MAGIC pat_mrn_id,               
# MAGIC pat_enc_csn_id,    
# MAGIC birth_dt,
# MAGIC death_dt,
# MAGIC pat_class_abbr,	
# MAGIC hosp_admsn_tm,	
# MAGIC hosp_disch_tm,
# MAGIC dept_abbr,
# MAGIC loc_abbr,
# MAGIC room_nm,
# MAGIC bed_label,
# MAGIC bed_stat,
# MAGIC sex_abbr,
# MAGIC means_of_arrv_abbr,
# MAGIC acuity_level_abbr, 
# MAGIC ed_dsptn_abbr,
# MAGIC disch_dsptn_abbr,
# MAGIC adt_arvl_tm,
# MAGIC hsp_acct_id,
# MAGIC accommodation_abbr,
# MAGIC user_id,
# MAGIC current_timestamp        as row_insert_tsp,
# MAGIC current_timestamp        as row_updt_tsp,
# MAGIC current_user             as insert_user_id,
# MAGIC current_user             as updt_user_id,
# MAGIC current_timestamp               as msg_enqueued_tsp,
# MAGIC null as cncl_admsn_flg,  --- We are not bringing the cancelled admissions as they are not required, so this will be null from the code.
# MAGIC pregnancy_flg
# MAGIC from adt_dtl
# MAGIC where row_num = 1 --- getting only the final bed and room details from previous day
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into epic_rltm.adt_hist
# MAGIC select msg_typ,
# MAGIC msg_src,
# MAGIC trigger_evnt,  
# MAGIC msg_tm,
# MAGIC msg_nm,
# MAGIC bod_id,
# MAGIC pat_mrn_id,               
# MAGIC pat_enc_csn_id,    
# MAGIC birth_dt,
# MAGIC death_dt,
# MAGIC pat_class_abbr,	
# MAGIC hosp_admsn_tm,	
# MAGIC hosp_disch_tm,
# MAGIC dept_abbr,
# MAGIC loc_abbr,
# MAGIC room_nm,
# MAGIC bed_label,
# MAGIC bed_stat,
# MAGIC sex_abbr,
# MAGIC means_of_arrv_abbr,
# MAGIC acuity_level_abbr, 
# MAGIC ed_dsptn_abbr,
# MAGIC disch_dsptn_abbr,
# MAGIC adt_arvl_tm,
# MAGIC hsp_acct_id,
# MAGIC accommodation_abbr,
# MAGIC user_id,
# MAGIC row_insert_tsp,
# MAGIC row_updt_tsp,
# MAGIC insert_user_id,
# MAGIC updt_user_id,
# MAGIC msg_enqueued_tsp,
# MAGIC pregnancy_flg 
# MAGIC from epic_rltm.adt_dtl;

# COMMAND ----------

# MAGIC %sql
# MAGIC ---#2: epic_rltm.encntr_visit_rsn
# MAGIC insert into epic_rltm.encntr_visit_rsn_hist
# MAGIC   select    distinct
# MAGIC             adt.msg_src                     as msg_src,
# MAGIC             adt.msg_tm                      as msg_tm,
# MAGIC             adt.pat_enc_csn_id              as pat_enc_csn_id,
# MAGIC             cl_rsn_visit.reason_visit_name  as encntr_rsn,
# MAGIC             current_timestamp               as row_insert_tsp,
# MAGIC             current_timestamp               as row_updt_tsp,
# MAGIC             current_user                    as insert_user_id,
# MAGIC             current_user                    as updt_user_id,
# MAGIC             current_timestamp               as msg_enqueued_tsp
# MAGIC  from          epic_rltm.adt_dtl            adt
# MAGIC     inner join clarity.pat_enc_rsn_visit rsn_visit    on adt.pat_enc_csn_id = rsn_visit.pat_enc_csn_id
# MAGIC     inner join clarity.cl_rsn_for_visit  cl_rsn_visit on rsn_visit.enc_reason_id = cl_rsn_visit.reason_visit_id
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC ---#2: epic_rltm.encntr_visit_rsn
# MAGIC insert into epic_rltm.encntr_visit_rsn
# MAGIC select * from epic_rltm.encntr_visit_rsn_hist;

# COMMAND ----------

# MAGIC %sql
# MAGIC ---#3: epic_rltm.encntr_dx
# MAGIC insert into epic_rltm.encntr_dx_hist
# MAGIC select 
# MAGIC             adt.msg_src                     as msg_src,
# MAGIC             adt.msg_tm                      as msg_tm,
# MAGIC             adt.pat_enc_csn_id              as pat_enc_csn_id,
# MAGIC             icd10.code						as dx_icd_cd,
# MAGIC             edg.dx_name             		as dx_nm,
# MAGIC             'ICD-10-CM'						as dx_cd_typ,
# MAGIC             current_timestamp               as row_insert_tsp,
# MAGIC             current_timestamp               as row_updt_tsp,
# MAGIC             current_user                    as insert_user_id,
# MAGIC             current_user                    as updt_user_id,
# MAGIC             current_timestamp               as msg_enqueued_tsp
# MAGIC from        epic_rltm.adt_dtl        adt
# MAGIC inner join clarity.pat_enc_dx        enc_dx  on adt.pat_enc_csn_id =  enc_dx.pat_enc_csn_id
# MAGIC inner join clarity.edg_current_icd10 icd10  on icd10.dx_id = enc_dx.dx_id
# MAGIC inner join clarity.clarity_edg       edg    on icd10.dx_id = edg.dx_id
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC ---#3: epic_rltm.encntr_dx
# MAGIC insert into epic_rltm.encntr_dx
# MAGIC select * from epic_rltm.encntr_dx_hist

# COMMAND ----------

# MAGIC %sql
# MAGIC ---#4: epic_rltm.encntr_er_complnt
# MAGIC insert into epic_rltm.encntr_er_complnt_hist
# MAGIC   select    distinct
# MAGIC             adt.msg_src                     as msg_src,
# MAGIC             adt.msg_tm                      as msg_tm,
# MAGIC             adt.pat_enc_csn_id              as pat_enc_csn_id,
# MAGIC             er_com.er_complaint             as er_complnt,
# MAGIC             current_timestamp               as row_insert_tsp,
# MAGIC             current_timestamp               as row_updt_tsp,
# MAGIC             current_user                    as insert_user_id,
# MAGIC             current_user                    as updt_user_id,
# MAGIC             current_timestamp               as msg_enqueued_tsp
# MAGIC from       epic_rltm.adt_dtl         adt
# MAGIC     inner join clarity.pat_enc_er_complnt er_com    on adt.pat_enc_csn_id = 	er_com.pat_enc_csn_id
# MAGIC ;  

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into epic_rltm.encntr_er_complnt
# MAGIC select * from epic_rltm.encntr_er_complnt_hist

# COMMAND ----------

# MAGIC %sql
# MAGIC ---#5: epic_rltm.encntr_nte
# MAGIC insert into epic_rltm.encntr_nte_hist
# MAGIC with nte as (select distinct
# MAGIC  hi.note_id
# MAGIC ,hi.pat_id
# MAGIC ,hi.pat_enc_csn_id
# MAGIC ,hi.crt_inst_local_dttm
# MAGIC ,nei.contact_serial_num
# MAGIC ,nei.pat_enc_csn_id       as note_enc_csn_id
# MAGIC ,hi.note_type_noadd_c
# MAGIC ,nei.ent_inst_local_dttm
# MAGIC ,row_number() over (partition by hi.pat_enc_csn_id  order by  nei.ent_inst_local_dttm desc) as row_num  
# MAGIC from              epic_rltm.adt_dtl       adt
# MAGIC inner join        clarity.hno_info          hi  on adt.pat_enc_csn_id = hi.pat_enc_csn_id
# MAGIC inner join        clarity.note_enc_info     nei on nei.note_id        = hi.note_id
# MAGIC  where   cast(nei.entry_instant_dttm as date)  >= to_date('2021-07-01','yyyy-MM-dd')
# MAGIC     and (nei.note_status_c is null or nei.note_status_c not in ('1','4','10'))
# MAGIC     and  hi.ip_note_type_c in ('6','8')      --- ('ED Notes','ED Triage Notes')
# MAGIC )
# MAGIC , nte_txt as (select pat_enc_csn_id
# MAGIC     ,  concat_ws('^',collect_list(note_text)) as note_text
# MAGIC     from (select t.pat_enc_csn_id
# MAGIC             ,hno.line         
# MAGIC             ,hno.note_text 
# MAGIC             from nte t
# MAGIC             inner join   clarity.hno_note_text     hno on hno.note_csn_id  = t.contact_serial_num
# MAGIC             where t.row_num = 1  -- Most Recent note
# MAGIC             order by hno.line asc
# MAGIC )
# MAGIC group by pat_enc_csn_id
# MAGIC order by pat_enc_csn_id asc
# MAGIC )
# MAGIC select 
# MAGIC adt.msg_src                     as msg_src,
# MAGIC adt.msg_tm                      as msg_tm,
# MAGIC nte_txt.pat_enc_csn_id          as pat_enc_csn_id,
# MAGIC nte_txt.note_text               as nte_txt,
# MAGIC 'ED'                            as nte_typ,
# MAGIC current_timestamp               as row_insert_tsp,
# MAGIC current_timestamp               as row_updt_tsp,
# MAGIC current_user                    as insert_user_id,
# MAGIC current_user                    as updt_user_id,
# MAGIC current_timestamp               as msg_enqueued_tsp
# MAGIC from nte_txt
# MAGIC inner join   epic_rltm.adt_dtl  adt on nte_txt.pat_enc_csn_id =  adt.pat_enc_csn_id
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into epic_rltm.encntr_nte
# MAGIC select * from epic_rltm.encntr_nte_hist

# COMMAND ----------

# MAGIC %md
# MAGIC #####STEP5 - Run Below Commands to move data from Delta tables to Synapse

# COMMAND ----------

# MAGIC %run ../../../etl/data_engineering/commons/utilities

# COMMAND ----------

adt_dtl_df = spark.sql("select * from epic_rltm.adt_dtl")
write_to_synapse(adt_dtl_df, 'epic_rltm.adt_dtl')

# COMMAND ----------

adt_hist_df = spark.sql("select * from epic_rltm.adt_hist")
write_to_synapse(adt_hist_df, 'epic_rltm.adt_hist')

# COMMAND ----------

encntr_dx_hist_df = spark.sql("select * from epic_rltm.encntr_dx_hist")
write_to_synapse(encntr_dx_hist_df, 'epic_rltm.encntr_dx_hist')

# COMMAND ----------

encntr_dx_df = spark.sql("select * from epic_rltm.encntr_dx")
write_to_synapse(encntr_dx_df, 'epic_rltm.encntr_dx')

# COMMAND ----------

encntr_visit_rsn_hist_df = spark.sql("select * from epic_rltm.encntr_visit_rsn_hist")
write_to_synapse(encntr_visit_rsn_hist_df, 'epic_rltm.encntr_visit_rsn_hist')

# COMMAND ----------

encntr_visit_rsn_df = spark.sql("select * from epic_rltm.encntr_visit_rsn")
write_to_synapse(encntr_visit_rsn_df, 'epic_rltm.encntr_visit_rsn')

# COMMAND ----------

encntr_er_complnt_hist_df = spark.table('epic_rltm.encntr_er_complnt_hist')
write_to_synapse(encntr_er_complnt_hist_df, 'epic_rltm.encntr_er_complnt_hist',mode='append',delete_query=f'delete from epic_rltm.encntr_er_complnt_hist where insert_user_id is not null')

# COMMAND ----------

# changing the order of columns in dataframe to match with column order in Synapse table
encntr_er_complnt_df = spark.table('epic_rltm.encntr_er_complnt')
# encntr_er_complnt_df = encntr_er_complnt_df.select('msg_src','msg_tm','pat_enc_csn_id','er_complnt','row_insert_tsp','insert_user_id','row_updt_tsp','updt_user_id')
write_to_synapse(encntr_er_complnt_df, 'epic_rltm.encntr_er_complnt',mode='append',delete_query=f'delete from epic_rltm.encntr_er_complnt where insert_user_id is not null')


# COMMAND ----------

encntr_nte_hist_df = spark.table('epic_rltm.encntr_nte_hist')
write_to_synapse(encntr_nte_hist_df, 'epic_rltm.encntr_nte_hist',mode='append',delete_query=f'delete from epic_rltm.encntr_nte_hist where insert_user_id is not null')


# COMMAND ----------

encntr_nte_df = spark.table('epic_rltm.encntr_nte')
write_to_synapse(encntr_nte_df, 'epic_rltm.encntr_nte',mode='append',delete_query=f'delete from epic_rltm.encntr_nte where insert_user_id is not null')
