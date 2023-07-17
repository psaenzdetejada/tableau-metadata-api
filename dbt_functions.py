import requests
import json
import pandas as pd

def get_account_id(token):
    
    url = "https://cloud.getdbt.com/api/v2/accounts/"

    payload={}
    headers = {
      'Content-Type': 'appication/json',
      'Authorization': 'Token '+ token
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    # print(response.text)
    response_json = json.loads(response.text)
    account_id = response_json['data'][0]['id']
    return(account_id)

def get_jobs(account_id, token):
    url = "https://cloud.getdbt.com/api/v2/accounts/"+account_id+"/jobs"

    payload={}
    headers = {
      'Content-Type': 'appication/json',
      'Authorization': 'Token '+ token
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    response_json = json.loads(response.text)
    print(response_json)
    job_dataframe = pd.json_normalize(response_json['data'])
    name_plus_job_id = job_dataframe[['id','name']]
    return(name_plus_job_id)

token = "28fd50d4ef314faef3170acdac17f2f5ddb95e4a"
accountId = get_account_id(token)

myJobs = get_jobs(str(accountId), token)
print(myJobs)