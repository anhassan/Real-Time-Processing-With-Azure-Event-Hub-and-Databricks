# Databricks notebook source
# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

import smtplib 
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from pyspark.sql.functions import *


from_email = 'DoNotReply@Mercy.Net'
to_email = 'EDAO_DSGM_RealTime@Mercy.Net'

database_name = 'epic_rltm'

tables = ["adt_hist", "encntr_dx_hist", "encntr_er_complnt_hist", "encntr_visit_rsn_hist", "encntr_nte_hist", "adt_dtl", "encntr_dx", "encntr_er_complnt", "encntr_visit_rsn", "encntr_nte"]

databricks_query = r"""
  with tmp (row_updt_tsp, msg_tm, msg_enqueued_tsp, tmstmp) as 
  (
    select max(row_updt_tsp) as row_updt_tsp, max(msg_tm) as msg_tm, max(msg_enqueued_tsp) as msg_enqueued_tsp, current_timestamp as tmstmp 
    from {database_name}.{table_name}
  )
  select
    'Databricks' as platform,
    '{table_name}' as table_name, 
    cast(tmp.row_updt_tsp as string) as row_updt_tsp,
    cast(tmp.msg_tm as string) as msg_tm,
    cast(tmp.tmstmp as string) as tmstmp,
    cast(tmp.msg_enqueued_tsp as string) as msg_enqueued_tsp,
    datediff(second, tmp.msg_tm, tmp.row_updt_tsp) as diff_row_msg,
    datediff(second, tmp.row_updt_tsp, tmp.tmstmp) as diff_tms_row,
    datediff(second, tmp.msg_tm, tmp.tmstmp) as diff_tms_msg,
    datediff(second, tmp.msg_enqueued_tsp, tmp.row_updt_tsp) as diff_row_met,
    datediff(second, tmp.msg_enqueued_tsp, tmp.tmstmp) as diff_tms_met
  from
    tmp
  where
    datediff(minute, tmp.msg_tm, tmp.row_updt_tsp) > 5
    or datediff(minute, tmp.msg_tm, tmp.tmstmp) > 5
    or datediff(minute, tmp.row_updt_tsp, tmp.tmstmp) > 5
    or datediff(minute, tmp.msg_enqueued_tsp, tmp.row_updt_tsp) > 5
    or datediff(minute, tmp.msg_enqueued_tsp, tmp.tmstmp) > 5
"""

synapse_query = """
  select
    'Synapse' as platform, 
    '{table_name}' as table_name, 
    tmp.row_updt_tsp as row_updt_tsp,
    tmp.msg_tm as msg_tm,
    tmp.tmstmp as tmstmp,
    tmp.msg_enqueued_tsp as msg_enqueued_tsp,
    DATEDIFF(SECOND, tmp.msg_tm, tmp.row_updt_tsp) as diff_row_msg,
    DATEDIFF(SECOND, tmp.row_updt_tsp, tmp.tmstmp) as diff_tms_row,
    DATEDIFF(SECOND, tmp.msg_tm, tmp.tmstmp) as diff_tms_msg,
    DATEDIFF(SECOND, tmp.msg_enqueued_tsp, tmp.row_updt_tsp) as diff_row_met,
    DATEDIFF(SECOND, tmp.msg_enqueued_tsp, tmp.tmstmp) as diff_tms_met
  from (
        select max(row_updt_tsp) as row_updt_tsp, max(msg_tm) as msg_tm, max(msg_enqueued_tsp) as msg_enqueued_tsp, cast(SYSDATETIMEOFFSET() at time zone 'Central Standard Time' as datetime) as tmstmp
        from {database_name}.{table_name}
  ) tmp
  where 
    datediff(minute, tmp.msg_tm, tmp.row_updt_tsp) > 5
    or datediff(minute, tmp.msg_tm, tmp.tmstmp) > 5
    or datediff(minute, tmp.row_updt_tsp, tmp.tmstmp) > 5
    or datediff(minute, tmp.msg_enqueued_tsp, tmp.row_updt_tsp) > 5
    or datediff(minute, tmp.msg_enqueued_tsp, tmp.tmstmp) > 5
"""

# COMMAND ----------

def generate_table_html(rows):
  
  rows = '\n'.join([
    f'''
      <tr>
        <td>{platform}</td>
        <td>{table_name}</td>
        <td>{row_updt_tsp}</td>
        <td>{msg_tm}</td>
        <td>{current_tsp}</td>
        <td>{msg_enqueued_tsp}</td>
        <td style="background-color:#C34A2C">{diff_updt_msg_tsp}</td>
        <td>{diff_curr_updt_tsp}</td>
        <td>{diff_curr_msg_tm}</td>
        <td>{diff_row_met}</td>
        <td>{diff_cur_met}</td>
      </tr>
    '''
    for platform, table_name, row_updt_tsp, msg_tm, current_tsp, msg_enqueued_tsp,
        diff_updt_msg_tsp, diff_curr_updt_tsp, diff_curr_msg_tm, diff_row_met, diff_cur_met in rows
  ])
  
  table_html = f'''
  <table border=1>
    <thead>
      <tr>
        <th>Platform</th>
        <th>TableName</th>
        <th>RowUpdateTimestamp</th>
        <th>MessageTimestamp</th>
        <th>CurrentTimestamp</th>
        <th>PublishedTS</th>
        <th>RowUpdateTimestamp - MessageTimestamp</th>
        <th>CurrentTimestamp - RowUpdateTimestamp</th>
        <th>CurrentTimestamp - MessageTimestamp</th>
        <th>RowUpdateTimestamp - PublishedTS</th>
        <th>CurrentTimestamp - PublishedTS</th>
      </tr>
    </thead>
    <tbody>
      {rows}
    </tbody>
  '''

  return table_html

# COMMAND ----------

def send_email(from_email, to_email, message):
  
  msg = MIMEMultipart()
  msg['Subject'] = '{} Environment - Service Bus tables that exceeds 5 minutes interval - Synapse & Databricks'.format(environment)

  recipients = [to_email]
  msg['From'] = from_email
  msg['To'] = ', '.join(recipients)
  title = 'Service Bus tables that exceeds 5 minutes interval - Synapse & Databricks'
  
  msg_content = '''<html>
    <head></head>
    <body>
      <h1>{0}</h1>
      <p>{1}</p>
    </body>
  </html>'''.format(title, message)
  
  msg.attach(MIMEText(msg_content, 'html'))
 
  s = smtplib.SMTP('smtp.mercy.net', 25)
  s.send_message(msg)
  s.quit()

# COMMAND ----------

def get_databricks_tables(database_name, tables, databricks_query):

  df = spark.sql(databricks_query.format(database_name=database_name, table_name=tables[0]))

  for table_name in tables[1:]:
    df = df.union(spark.sql(databricks_query.format(database_name=database_name, table_name=table_name)))
  
  return df.collect()


def get_synapse_tables(database_name, tables, synapse_query):
  
  spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", access_key)
  tempDir = "abfss://" + synapse_container + "@"+ storage_account_name + ".dfs.core.windows.net/temp-data"  
  
  synapse_query = '\n union all \n'.join([synapse_query.format(database_name=database_name, table_name=table_name) for table_name in tables])
  
  return spark.read \
      .format("com.databricks.spark.sqldw") \
      .option("url", synapse_jdbc) \
      .option("tempDir", tempDir) \
      .option("useAzureMSI", "true") \
      .option("query", synapse_query) \
      .load() \
      .collect()

# COMMAND ----------

databricks_tables = get_databricks_tables(database_name, tables, databricks_query)
synapse_tables = get_synapse_tables(database_name, tables, synapse_query)

missing_tables = databricks_tables + synapse_tables

if missing_tables:
  missing_tables.sort(key=lambda x: (x['platform'], x['table_name']))
  send_email(from_email, to_email, generate_table_html(missing_tables))
else:
  print('No Table is running late.')