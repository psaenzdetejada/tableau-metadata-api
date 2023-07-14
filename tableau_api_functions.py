"""The following code defines several functions to connect to Tableau Cloud REST API, Snowflake and feed Tableau Cloud Metadata
available with Data Management with information already in Snowflake column comments, table comments, etc.
To understand Tableau REST API errors, in case they occur, we recommend visiting the following link:
https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_errors.htm#common-errors

This code was developed by Pablo Sáenz de Tejada (https://github.com/psaenzdetejada). 
I am not a professional Python developer, so this code might be far from optimized and does not necessarily follow best practices.
"""
# Import the required python modules
import requests
import json
import pandas as pd

def authenticate_tableau(cloud_pod,content_url,token_name,pat_secret,api_version):
    """Authenticate to Tableau Cloud.:

    Parameters
    ----------
    cloud_pod: str
      Url of the Tableau Cloud Site Pod. 
      Example: dub01.online.tableau.com
    content_url: str
      The permanent name of the site to sign in to. The content URL appears in the URL path of Tableau content in your browser address bar after the Tableau Cloud URL.
      Example: mytableaucloudsite  
    token_name: str
      The name of the personal access token. The token name is available on a user’s account page on Tableau Cloud.
      Example: mytoken
    pat_secret: str
      The secret value of the personal access token. The value of the secret is available only in the dialog that appears when a user creates a personal access token. 
      More info can be found at: https://help.tableau.com/current/pro/desktop/en-us/useracct.htm#pat
      Example: 234refasdf-23rdfsaf-23rfeasdfsa
    api_version: str
      Version of the Tableau REST API you want to use. When publishing this code, latest version was 3.20.
      More info about versions can be found at https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_versions.htm#version_and_rest
      Example: 3.20
    
    Returns
    -------
    Returns two values: mySiteId,mySessionToken
    mySiteId is the internal 
    """  
    url = "https://" + cloud_pod + "/api/"+ api_version +"/auth/signin"

    payload = json.dumps({
      "credentials": {
        "personalAccessTokenName": token_name,
        "personalAccessTokenSecret": pat_secret,
        "site": {
          "contentUrl": content_url
        }
      }
    })
    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    jsonresponse = response.json()
    response_txt = str(response)
    if "200" in response_txt:
        mySiteId=jsonresponse['credentials']['site']['id']
        mySessionToken=jsonresponse['credentials']['token']
        myUserId=jsonresponse['credentials']['user']['id']
        print("\n----------Successful authentication in Tableau Cloud.\n")
        return mySiteId,mySessionToken,myUserId
    else:
        print("\n----------Error when authenticating in Tableau Cloud:", response_txt + "\n")

def get_projects(url_name,token_name,site_id):
    url = "https://" + url_name + "/api/3.20/sites/" + site_id + "/projects" 
    print("Your Project URL: " + url)

    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-Tableau-Auth': token_name
      }

    response = requests.request("GET", url, headers=headers)
    print("Response code:", response)
    jsonresponse = response.json()
    print(jsonresponse)

# correct!
def tableau_get_tables_external_assets(token, pod, siteid, schema):
    url = "https://" + pod + "/api/3.20/sites/" + siteid + "/tables" 

    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-Tableau-Auth': token
      }

    response = requests.request("GET", url, headers=headers)
    print("Response code:", response)
    jsonresponse = response.json()
    df = pd.DataFrame(jsonresponse["tables"]["table"])
    columns_to_keep = ["id", "name", "schema"]
    df = df[columns_to_keep]
    return df

#CORRECT!
def tableau_get_columns_from_table(token, pod, siteid, tableid):
    url = "https://" + pod + "/api/3.20/sites/" + siteid + "/tables/" + tableid + "/columns"

    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-Tableau-Auth': token
    }

    response = requests.request("GET", url, headers=headers)                                                                
    jsonresponse = response.json()
    df = pd.DataFrame(jsonresponse["columns"]["column"])
    return df

def tableau_update_table_description(cloud_pod,site_id,table_id,api_version,token,description,row_count=None):
    update_table_description = "https://"+cloud_pod+"/api/"+api_version+"/sites/"+site_id+"/tables/"+table_id

    if row_count is None:
        description
    else:
        description=description+" and the number of rows is "+str(row_count)

    payload = json.dumps({
      "table": {
        "description": description
      }
    })

    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-Tableau-Auth': token
    }

    columns_response = requests.request("PUT", update_table_description, headers=headers, data=payload)                                                                
    jsonresponse=columns_response.json()
    print(jsonresponse)
    return jsonresponse

def tableau_add_bigdata_label(cloud_pod,site_id,table_id,api_version,token,description,row_count=None):
    update_table_description = "https://"+cloud_pod+"/api/"+api_version+"/sites/"+site_id+"/tables/"+table_id

    if row_count is None:
        description
    else:
        description=description+" and the number of rows is "+str(row_count)

    payload = json.dumps({
      "table": {
        "description": description
      }
    })

    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-Tableau-Auth': token
    }

    columns_response = requests.request("PUT", update_table_description, headers=headers, data=payload)                                                                
    jsonresponse=columns_response.json()
    print(jsonresponse)
    return jsonresponse

def tableau_createupdate_labelvalue(cloud_pod,site_id,api_version,token,labelvalue,description):
    updatecreate_labelvalue = "https://"+cloud_pod+"/api/"+api_version+"/sites/"+site_id+"/labelValues"

    payload = json.dumps({
      "labelValue": {
        "name": labelvalue,
        "category": "sensitivity",
        "description": description
      }
    })

    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-Tableau-Auth': token
    }

    columns_response = requests.request("PUT", updatecreate_labelvalue, headers=headers, data=payload)                                                                
    jsonresponse=columns_response.json()
    print(jsonresponse)
    return jsonresponse

def tableau_list_labelvalues(cloud_pod,site_id,api_version,token):
    list_labelvalues = "https://"+cloud_pod+"/api/"+api_version+"/sites/"+site_id+"/labelValues"

    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-Tableau-Auth': token
    }

    response = requests.request("GET", list_labelvalues, headers=headers)                                                                
    response = response.json()
    print("Response code:", response)
    return response

def tableau_add_tag_column(token, cloud_pod, api_version, siteid, columnid, mytag):
    url = "https://" + cloud_pod + "/api/" + api_version + "/sites/" + siteid + "/columns/" + columnid + "/tags"

    payload = json.dumps({
      "tags": {
        "tag": [
            {
            "label": mytag
          }
        ]
      }
    })

    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-Tableau-Auth': token
    }

    response = requests.request("PUT", url, headers=headers, data=payload)                                                                
    jsonresponse = response.json()
    print(jsonresponse)
    return jsonresponse

def tableau_add_data_warning(token, cloud_pod, api_version, siteid, columnid, label):
    url = "https://" + cloud_pod + "/api/" + api_version + "/sites/" + siteid + "/dataQualityWarnings/column/" + columnid

    payload = json.dumps({
      "dataQualityWarning": {
          "type": label,
          "message": "Test"
          }
        })

    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-Tableau-Auth': token
    }

    response = requests.request("POST", url, headers=headers, data=payload)                                                                
    jsonresponse = response.json()
    print(jsonresponse)
    return jsonresponse