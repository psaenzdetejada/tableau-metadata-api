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

def authenticate_tableau(pod, content_url, token_name, pat_secret, api_version):
    """Authenticate to Tableau Cloud.:

    Parameters
    ----------
    pod: str
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
    Returns three values: mySiteId,mySessionToken, myUserId. The first two are used in all subsequent API calls.
    
    mySiteId: str
      Is the internal id of Tableau's Site. Required for most of the APIs.

    mySessionToken: str
      The X-Tableau-Auth token. This is a credentials token that you use in subsequent calls to the APIs.

    myUserId: str
      The internal Tableau Id of the user used to signed in.  
    """
    url = "https://" + pod + "/api/"+ api_version +"/auth/signin"

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

    # The function checks if the response to the API is OK (code 200). In that case, returns the values required for subsequent API calls. 
    # If the call is not OK, it will print the specific error.
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

def get_tableau_tables_external_assets(token, pod, site_id, api_version):
    """Get External Assets Tables from Tableau:

    Parameters
    ----------
    token: str
      The mySessionToken (X-Tableau-Auth Token) returned by the authenticate_tableau function.
    pod: str
      Url of the Tableau Cloud Site Pod. 
      Example: dub01.online.tableau.com
    site_id: str
      The mySiteId luid returned by the authenticate_tableau function.
      Example: e2a91ed0-3ad4-4e7b-5df1-8d54b42c6ab7
    api_version: str
      Version of the Tableau REST API you want to use. When publishing this code, latest version was 3.20.
      More info about versions can be found at https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_versions.htm#version_and_rest
      Example: 3.20
    
    Returns
    -------
    DataFrame with all Table External Assets in the Tableau Site. Dropping some unnecesary columns might be recommended.
    """
    url = "https://" + pod + "/api/" + api_version + "/sites/" + site_id + "/tables" 

    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-Tableau-Auth': token
      }

    response = requests.request("GET", url, headers=headers)
    print("get_tableau_tables_external_assets returned the following response code:", response, "\n")
    jsonresponse = response.json()
    df = pd.DataFrame(jsonresponse["tables"]["table"])
   # columns_to_keep = ["id", "name", "schema"]
   # df = df[columns_to_keep]
    return df

def update_tableau_table_description(token, pod, site_id, table_id, api_version, description, row_count=None):
    """Updates a Table Description in Tableau:

    Parameters
    ----------
    token: str
      The mySessionToken (X-Tableau-Auth Token) returned by the authenticate_tableau function.
    pod: str
      Url of the Tableau Cloud Site Pod. 
      Example: dub01.online.tableau.com
    site_id: str
      The mySiteId luid returned by the authenticate_tableau function.
      Example: e2a91ed0-3ad4-4e7b-5df1-8d54b42c6ab7
    table_id:
      The Tableau REST API table luid (for example, from the get_tableau_tables_external_assets function) target to update table description.
    api_version: str
      Version of the Tableau REST API you want to use. When publishing this code, latest version was 3.20.
      More info about versions can be found at https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_versions.htm#version_and_rest
      Example: 3.20
    
    Returns
    -------
    Json response with the result of the PUT request.
    """
    url = "https://" + pod + "/api/" + api_version + "/sites/" + site_id + "/tables/" + table_id

    if row_count is None:
        description
    else:
        description = description + "\nTotal table rows is "+str(row_count) + "."

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

    response = requests.request("PUT", url, headers=headers, data=payload)                                                                
    jsonresponse = response.json()
    response_txt = str(response)
    if "200" in response_txt:
      print("\n----------Successfully updated table " + table_id + " description.")
    else:
      print("\n----------Error when updating table " + table_id + " description. Error " + str(response))
    
    return jsonresponse

def get_tableau_columns_from_table(token, pod, site_id, table_id, api_version):
    """Get all Tableau's column metadata details from a specific Table:

    Parameters
    ----------
    token: str
      The mySessionToken (X-Tableau-Auth Token) returned by the authenticate_tableau function.
    pod: str
      Url of the Tableau Cloud Site Pod. 
      Example: dub01.online.tableau.com
    site_id: str
      The mySiteId luid returned by the authenticate_tableau function.
      Example: e2a91ed0-3ad4-4e7b-5df1-8d54b42c6ab7
    table_id:
      The Tableau REST API table luid (for example, from the get_tableau_tables_external_assets function) target to update table description.
    api_version: str
      Version of the Tableau REST API you want to use. When publishing this code, latest version was 3.20.
      More info about versions can be found at https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_versions.htm#version_and_rest
      Example: 3.20
    
    Returns
    -------
    DataFrame with all column metadata from the table_id in the Tableau Site. Dropping some unnecesary columns might be recommended.
    """
    url = "https://" + pod + "/api/" + api_version + "/sites/" + site_id + "/tables/" + table_id + "/columns"

    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-Tableau-Auth': token
    }

    response = requests.request("GET", url, headers=headers)                                                                
    jsonresponse = response.json()
    df = pd.DataFrame(jsonresponse["columns"]["column"])
    return df

def update_tableau_columns_descriptions(token, pod, site_id, table_id, column_id, api_version, description):
    """Updates a column description from a concrete table in Tableau:

    Parameters
    ----------
    token: str
      The mySessionToken (X-Tableau-Auth Token) returned by the authenticate_tableau function.
    pod: str
      Url of the Tableau Cloud Site Pod. 
      Example: dub01.online.tableau.com
    site_id: str
      The mySiteId luid returned by the authenticate_tableau function.
      Example: e2a91ed0-3ad4-4e7b-5df1-8d54b42c6ab7
    table_id:
      The Tableau REST API table luid (for example, from the get_tableau_tables_external_assets function) of the column to update.
    column_id:
      The Tableau REST API column luid (for example, from the get_tableau_columns_from_table function) target to update column description.
    api_version: str
      Version of the Tableau REST API you want to use. When publishing this code, latest version was 3.20.
      More info about versions can be found at https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_versions.htm#version_and_rest
      Example: 3.20
    
    Returns
    -------
    Json response with the result of the PUT request.
    """
    url = "https://" + pod + "/api/" + api_version + "/sites/" + site_id + "/tables/" + table_id + "/columns/" + column_id

    payload = json.dumps({
      "column": {
        "description": description
      }
    })

    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-Tableau-Auth': token
    }

    response = requests.request("PUT", url, headers=headers, data=payload)                                                                
    jsonresponse = response.json()
    response_txt = str(response)
    if "200" in response_txt:
      print("\n----------Successfully updated column " + column_id + " description in table " + table_id +".")
    else:
      print("\n----------Error when updating column " + column_id + " description. in table " +  table_id + "Error " + str(response))
    return jsonresponse

def update_tableau_column_labels(token, pod, site_id, api_version, asset_luid, labelvalue, labeldescription):
    """Updates a columns sensitivity label:

    Parameters
    ----------
    token: str
      The mySessionToken (X-Tableau-Auth Token) returned by the authenticate_tableau function.
    pod: str
      Url of the Tableau Cloud Site Pod. 
      Example: dub01.online.tableau.com
    site_id: str
      The mySiteId luid returned by the authenticate_tableau function.
      Example: e2a91ed0-3ad4-4e7b-5df1-8d54b42c6ab7
    api_version: str
      Version of the Tableau REST API you want to use. When publishing this code, latest version was 3.20.
      More info about versions can be found at https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_versions.htm#version_and_rest
      Example: 3.20
    asset_luid:
      The Tableau REST API column luid target to update the sensitivity label.
    
    Returns
    -------
    Json response with the result of the PUT request.
    """
    url = "https://"+ pod + "/api/" + api_version + "/sites/" + site_id + "/labels"

    payload = json.dumps({
      "contentList": {
        "content": [
            {
            "contentType": "column",
            "id": asset_luid
          }
          ]
        },
      "label": {
          "value": labelvalue,
          "message": labeldescription
          }  
          }
    )
    
    headers = {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
      'X-Tableau-Auth': token
    }

    response = requests.request("PUT", url, headers=headers, data=payload)                                                                
    response = response.json()
    print("Response code:", response)
    return response