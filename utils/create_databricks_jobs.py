# Databricks notebook source
# MAGIC %run ../utils/switch_job_ownership

# COMMAND ----------

import json
import requests

# COMMAND ----------

def get_job_parameters(get_hooks,headers,job_id):
    try:
      job_params = requests.get(get_hooks.format(job_id),headers=headers).json()['settings']['tasks'][0]['notebook_task']['base_parameters']
    except:
      job_params = {}
    return job_params
  
def delete_job(get_hooks, headers, job_id):
  payload = json.dumps({
      "job_id": job_id
  })
  requests.post(get_hooks, headers=headers, data=payload)
  print("Deleted job with id : {}".format(job_id))
  
def get_cluster_id(cluster_name, databricks_instance_name=None):
  if databricks_instance_name is None:
    databricks_instance_name = dbutils.secrets.get(scope="databricks-credentials",key="databricks-instance-name")
  authorization_token = dbutils.secrets.get(scope="switch-ownership",key="databricks-auth-token")
  headers={"Authorization": "Bearer "+authorization_token}
  gethooks= "https://" + databricks_instance_name + "/api/2.0/clusters/list" 
  response = requests.get(gethooks, headers=headers)
  clusters_info = response.json()["clusters"]
  for cluster in clusters_info:
    if cluster["cluster_name"] == cluster_name:
      return cluster["cluster_id"]
  else:  
    raise Exception("Cluster : {} does not exist".format(cluster_name))    
      
def create_jobs(jobs_dict, cluster_name, overwrite=True, switch_ownership=True,
                databricks_instance_name = dbutils.secrets.get(scope="databricks-credentials",key="databricks-instance-name")):
  
  authorization_token = dbutils.secrets.get(scope="switch-ownership",key="databricks-auth-token")
  headers={"Authorization": "Bearer "+authorization_token}
  
  gethooks= "https://" + databricks_instance_name + "/api/2.0/jobs/list" 
  response = requests.get(gethooks, headers=headers)
  existing_jobs = [job['settings']['name']  for job in response.json()['jobs']]
  existing_job_ids = [job['job_id'] for job in response.json()['jobs']]

  get_hooks = "https://" + databricks_instance_name + "/api/2.1/jobs/get?job_id={}"
  existing_job_params = [ get_job_parameters(get_hooks,headers,job_id) for job_id in existing_job_ids]
  
  responses= []
    
  for job_name, key in jobs_dict.items():
    [notebook_path,notebook_parameters] = [key[0],key[1]] if isinstance(key,list) else [key,{}]
    print("Notebook_Params : ", notebook_parameters)
    if job_name in existing_jobs:
      print("Job with name {} already exists with parameters".format(existing_job_params[existing_jobs.index(job_name)]))
      if overwrite:
        print("Overwritting the Job with Job Name : {}".format(job_name))
        existing_job_id = existing_job_ids[existing_jobs.index(job_name)] 
        get_hooks = "https://" + databricks_instance_name + "/api/2.0/jobs/delete"
        headers={"Authorization": "Bearer {}".format(dbutils.secrets.get(scope="databricks-credentials",key="databricks-auth-token")), 'Content-type': 'application/json'}
        delete_job(get_hooks, headers, existing_job_id)
      else:
        print("Skipping Creation - Job : {} already exists and overwrite : {}".format(job_name,False))
        continue
    payload = json.dumps({
      "name": job_name,
      "notebook_task": {
        "notebook_path": notebook_path,
        "base_parameters": notebook_parameters
      },
      "existing_cluster_id": get_cluster_id(cluster_name)
    })
    
    gethooks= "https://" + databricks_instance_name + "/api/2.0/jobs/create"     #add your databricks workspace instance name over here
    headers={"Authorization": "Bearer {}".format(dbutils.secrets.get(scope="databricks-credentials",key="databricks-auth-token")), 'Content-type': 'application/json'}  
    
    response = requests.post(gethooks, headers=headers, data=payload)
    print("Job : {} created".format(job_name))
    responses.append(response.json())
    
    if switch_ownership:
      switch_databricks_job_ownership(job_name,cluster_name)
      print("Ownership of Job : {} shifted to service principal".format(job_name))
    
  return responses
