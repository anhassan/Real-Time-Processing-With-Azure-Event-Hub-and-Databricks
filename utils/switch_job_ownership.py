# Databricks notebook source
databricks_auth_token = dbutils.secrets.get(scope="switch-ownership",key="databricks-auth-token")
service_principal_client_id = dbutils.secrets.get(scope="switch-ownership",key="service-principle-client-id")
databricks_instance = dbutils.secrets.get(scope="databricks-credentials",key="databricks-instance-name")
service_principal_name = "switch_job_ownership"

# COMMAND ----------

# Unregister service principal from databricks workspace given the service principal name
def unregister_service_principal(service_principal_name):
  
  headers={"Authorization": "Bearer " + databricks_auth_token}
  get_service_principal_url = "https://{}/api/2.0/preview/scim/v2/ServicePrincipals".format(databricks_instance)
  try:
      registered_service_principal = json.loads(requests.get(get_service_principal_url,headers=headers).content)['Resources']
  except Exception as error:
      print("Request Error : ",error)
      return []
  try:
      service_principal_id = [service_principal['id'] for service_principal in registered_service_principal if service_principal['displayName'] == service_principal_name][0]
  except Exception as error:
      print ("No service principal registered in databricks workspace named : {}....".format(service_principal_name))
      return []
  try:
      unregister_service_principal_url = "https://{}/api/2.0/preview/scim/v2/ServicePrincipals/{}".format(databricks_instance,service_principal_id)
      response = requests.delete(unregister_service_principal_url,headers=headers)
      print("Response Unregistering Service Principal : ", json.loads(response.content))
      print("Unregistered service principal : {} from databricks workspace".format(service_principal_name))
  except Exception as error:
      print("Request Error : ",error)
      return []
    

# COMMAND ----------

# Register service principal via post request to the workspace
def register_service_principal(url,headers,payload,service_principal_name):
  try:
    response = requests.post(url,headers=headers,data=payload)
    #print("Response On Service Principle Registration : ", json.loads(response.content))
    #print("Service Principal with service principal name : {} registered in databricks".format(service_principal_name))
  except Exception as error:
    print("Request Error : ",error)
    raise Exception(error)

# Add service principal to the databricks workspace
def add_service_principal_in_workspace(service_principal_client_id,service_principal_name):
  
  headers = {"Authorization": "Bearer " + databricks_auth_token}
  service_principal_base_url = "https://{}/api/2.0/preview/scim/v2/ServicePrincipals".format(databricks_instance)
  payload_service_principal =  json.dumps({"schemas":[
        "urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"
      ],
      "applicationId": service_principal_client_id,
      "displayName": service_principal_name,
      "entitlements":[
        {
          "value":"allow-cluster-create"
        }
      ]})
  # check whether service principal exists in the databricks workspace already or not
  try:
      response = json.loads(requests.get(service_principal_base_url,headers=headers).content)
      #print("Response On whether Service Principle Already Registered : ",json.loads(requests.get(service_principal_base_url,headers=headers).content))
  except Exception as error:
    print("Request Error : ",error)
    raise Exception(error)
    
  if 'Resources' in response.keys():
    response_id_list = [service_principal['id'] for service_principal in response['Resources'] if service_principal['displayName'] == service_principal_name]
    #print("Response Id List : ",response_id_list)
    if len(response_id_list) <= 0 :
      # register the service principal in the databricks workspace as it is not registered before
      register_service_principal(service_principal_base_url,headers,payload_service_principal,service_principal_name)
        
    else:
      pass
      #print("Service Principal with service principal name : {} already registered in databricks....".format(service_principal_name))
  else:
    register_service_principal(service_principal_base_url,headers,payload_service_principal,service_principal_name)
  
  
  

# COMMAND ----------

# Grant notebook tied to the input job can_manage permissions
def grant_job_notebook_permissions(headers,service_principal_client_id,notebook_path):
  
  get_notebook_id_url = "https://{}/api/2.0/workspace/list?path={}".format(databricks_instance,notebook_path)
  try:
      notebook_id = json.loads(requests.get(get_notebook_id_url,headers=headers).content)['objects'][0]['object_id']
  except Exception as error:
      print("Request Error : ",error)
      return []
  payload_notebook = json.dumps({
  "access_control_list" : [{
    "service_principal_name" : service_principal_client_id,
    "permission_level" : "CAN_MANAGE" 
    }] 
  })
  change_notebook_permissions_url = "https://{}/api/2.0/preview/permissions/notebooks/{}".format(databricks_instance,notebook_id)
  try:
      response = requests.patch(change_notebook_permissions_url,headers=headers,data=payload_notebook)
      #print("Response Notebook Permission Json : ",json.loads(response.content))
      #print("Permission : CAN_MANAGE granted to {}".format(notebook_path))
  except Exception as error:
      print("Request Error :",error)
      raise Exception(error)

# COMMAND ----------


# Gets all the subpaths derived from a path
def get_all_valid_paths(input_path,delimiter="/"):
  end = len(input_path)
  valid_paths = []
  while end > 0 :
    input_path = input_path[0:end]
    valid_paths.append(input_path)
    end = input_path.rfind(delimiter)
  return valid_paths

# Get directories metadata given the directory name
def get_directories_metadata(input_path,accessible_directories):
  valid_paths = get_all_valid_paths(input_path)
  headers = {"Authorization": "Bearer " + databricks_auth_token}
  dirs_metadata = []
  for path in valid_paths:
    url = "https://{}/api/2.0/workspace/list?path={}".format(databricks_instance,path)
    try:
        response_list = json.loads(requests.get(url,headers=headers).content)['objects']
    except Exception as error:
        print("Request Error  : ",error)
    dirs_metadata += [(response['object_id'],response['object_type'],response['path']) for response in response_list if 
                      (response['object_type'] == "DIRECTORY" or response['object_type'] == "REPO")  and 
                       any(response['path'].endswith(directory) for directory in accessible_directories)]
    dirs_metadata = list(set(dirs_metadata))
    if len(dirs_metadata) == len(accessible_directories):
      return dirs_metadata

# Grant directories tied to the input job can_manage permissions
def grant_job_directory_permissions(headers,service_principal_client_id,directory_metadata):
  
  directory_type_dict = {"DIRECTORY":"directories","REPO":"repos"}
  directory_id,directory_type,directory_path = directory_metadata[0],directory_metadata[1],directory_metadata[2]
  payload_directory = json.dumps({
  "access_control_list" : [{
    "service_principal_name" : service_principal_client_id,
    "permission_level" : "CAN_MANAGE" 
    }] 
  })
  change_directory_permissions_url = "https://{}/api/2.0/preview/permissions/{}/{}".format(databricks_instance,directory_type_dict[directory_type],directory_id)
  try:
      response = requests.patch(change_directory_permissions_url,headers=headers,data=payload_directory)
      #print("Response Directory Permissions : ",json.loads(response.content))
      #print("Permission : CAN_MANAGE granted to {}".format(directory_path))
  except Exception as error:
      print("Request Error :",error)
      return []

# COMMAND ----------

def grant_all_repo_permissions(headers,service_principal_client_id):
  
  payload_directory = json.dumps({
  "access_control_list" : [{
    "service_principal_name" : service_principal_client_id,
    "permission_level": "CAN_MANAGE"
    }] 
  })
  try:
    all_dir_url = "https://{}/api/2.0/workspace/list?path={}".format(databricks_instance,"/Repos")
    response = json.loads(requests.get(all_dir_url,headers=headers).content)['objects']
    dir_paths = [obj["path"]for obj in response]
  except Exception as error:
    print("Request Error : ",error)
    raise Exception(error)
    
    
  repos_metadata = []
  for dir_path in dir_paths:
    try:
      all_repos_url = "https://{}/api/2.0/workspace/list?path={}".format(databricks_instance,dir_path)
      response = json.loads(requests.get(all_repos_url,headers=headers).content)
      try:
        response = response["objects"]
      except:
        response = []
      repos_metadata += [[obj["object_id"],obj["path"]]for obj in response]
    except Exception as error:
      print("Request Error : ",error)
      raise Exception(error)
      
  for repo_id,repo_path in repos_metadata:
    change_directory_permissions_url = "https://{}/api/2.0/preview/permissions/repos/{}".format(databricks_instance,repo_id)
    try:
        response = requests.put(change_directory_permissions_url,headers=headers,data=payload_directory)
        #print("Response Directory Permissions : ",json.loads(response.content))
        #print("Permission : CAN_MANAGE granted to {}".format(repo_path))
    except Exception as error:
        print("Request Error :",error)
        raise Exception(error)
  

# COMMAND ----------


# Grant service principle cluster access by providing the cluster name and the service principal name
def grant_cluster_permissions(cluster_name,service_principal_client_id):
  
  headers = {"Authorization": "Bearer " + databricks_auth_token}
  get_cluster_id_url = "https://{}/api/2.0/clusters/list".format(databricks_instance)
  try:
      response_list = json.loads(requests.get(get_cluster_id_url,headers=headers).content)['clusters']
  except Exception as error:
      print("Request Error : ",error)
      raise Exception(error)
  try:
      cluster_id = [cluster_details['cluster_id'] for cluster_details in response_list if cluster_details['cluster_name'] == cluster_name][0]
  except Exception as error:
      print("Cluster with Cluster Name : {} does not exists...".format(cluster_name))
      raise Exception(error)
  
  payload_cluster = json.dumps({
  "access_control_list" : [{
    "service_principal_name" : service_principal_client_id,
    "permission_level" : "CAN_MANAGE" 
    }] 
  })
  cluster_permissions_url = "https://{}/api/2.0/preview/permissions/clusters/{}".format(databricks_instance,cluster_id)
  try:
      response = requests.patch(cluster_permissions_url,headers=headers,data=payload_cluster)
      #print("Response Cluster Permissions : ",json.loads(response.content))
      #print("Permission : CAN_MANAGE granted to Cluster : {}".format(cluster_name))
  except Exception as error:
      print("Request Error :",error)
      raise Exception(error)

# COMMAND ----------

# Gives service principal is_owner permission on databricks job when provided the databricks job name along with service principal client id
def switch_databricks_job_ownership(job_name,cluster_name):
  
  headers = {"Authorization": "Bearer " + databricks_auth_token}
  
  # Add service principal to the workspace if its not added already
  add_service_principal_in_workspace(service_principal_client_id,service_principal_name)
  
  # Grant service principal rights to use the cluster
  grant_cluster_permissions(cluster_name,service_principal_client_id)
  
  list_jobs_url = "https://{}/api/2.1/jobs/list".format(databricks_instance)
  try:
      list_jobs_response = json.loads(requests.get(list_jobs_url,headers=headers).content)['jobs']
  except Exception as error:
      print("Request Error : ",error)
      raise Exception(error)
  try:
      job_id = [job_contents['job_id'] for job_contents in list_jobs_response if job_contents['settings']['name'] == job_name][0]
  except Exception as error:
      print("No job with job name : {} exists...".format(job_name))
      raise Exception(error)
  try:
      job_id_query = "https://{}/api/2.1/jobs/get?job_id={}".format(databricks_instance,job_id)
      notebook_path = json.loads(requests.get(job_id_query,headers=headers).content)['settings']['tasks'][0]['notebook_task']['notebook_path']
  except Exception as error:
      print("Request Error : ",error)
      raise Exception(erorr)
  
  change_ownership_url = "https://{}/api/2.0/preview/permissions/jobs/{}".format(databricks_instance,job_id)
  payload_change_ownership = json.dumps({
  "access_control_list" : [{
    "service_principal_name" : service_principal_client_id,
    "permission_level" : "IS_OWNER" 
    
    }] 
  })
  try:
      response = requests.put(change_ownership_url,headers=headers,data=payload_change_ownership)
      #print("Response On Job change : ",json.loads(response.content))
      #print("Service principal given IS_OWNER rights for databricks job for job name : {}".format(job_name))
  except Exception as error:
      print("Request Error : ",error)
      raise Exception(error)
  
  grant_all_repo_permissions(headers,service_principal_client_id)
  