# Import the required python modules
import requests
import json
import pandas as pd
import snowflake.connector

from tableau_api_functions import authenticate_tableau, get_tableau_tables_external_assets, update_tableau_table_description, get_tableau_columns_from_table, update_tableau_column_labels
from snowflake_functions import authenticate_snowflake, get_snowflake_table_column_tags

# Tableau Credentials
pod = "10ax.online.tableau.com"
siteName = "pablodevsite"
tokenName = "python"
tokenSecret  = "HkBVYdCvSiiqhnN5D+s2pw==:IQK9nZYsDUWVp1abTekpSsg2vkM5hdcF"
apiVersion = "3.20"

# Snowflake Credentials
snow_u = "pablosaenz"
snow_p = "9@x!oNgNWfV%Gt*Xg7O^9in#W!xq"
account_name = "hc16866.eu-west-1"


# Authenticate into Tableau Clouda and store the Ids and Token.
siteId, authToken, myUserId = authenticate_tableau(pod, siteName, tokenName, tokenSecret, apiVersion)

# Get all the External Assets, remove Columns to keep basic information: TableId, TableName, Schema.
external_assets = get_tableau_tables_external_assets(authToken, pod, siteId, "3.20")
external_assets = external_assets.drop(["site", "tags", "description", "certificationNote", "isEmbedded", "certifier", "location", "isCertified"], axis=1)
column_rename = {"id": "tableauTableId", "name": "tableauTableName", "schema": "tableauTableSchema"}
external_assets = external_assets.rename(columns=column_rename)
# print(external_assets)

# Optional Filter by Schema.
external_assets = external_assets.query("tableauTableSchema == 'PUBLIC'")
print(external_assets)

# Log in Snowflake and get tables from a concrete Database
auth_snow = authenticate_snowflake(snow_u, snow_p, account_name, "PERSONAL")

snow_column_tags = (get_snowflake_table_column_tags(auth_snow, "PERSONAL", "SLEEP"))

tableau_columns = pd.DataFrame()

for index, row in external_assets.iterrows():
    column_data = get_tableau_columns_from_table(authToken, pod, siteId, row["tableauTableId"], apiVersion)
    tableau_columns = pd.concat([tableau_columns, column_data])
    

print("Printing Column Data...")
tableau_columns = tableau_columns.drop(["site", "tags", "nullable", "remoteType", "description"], axis=1)
print(tableau_columns)

print("Snowflake Tags...")
print(snow_column_tags)

print("joining...")

join = pd.merge(
    external_assets, 
    snow_column_tags,
    how = "inner",
    left_on = ["tableauTableSchema", "tableauTableName"],
    right_on = ["OBJECT_SCHEMA", "OBJECT_NAME"]
)

print(join)

join_tableau = pd.merge(
    tableau_columns,
    join,
    how = "inner",
    left_on = ["parentTableId", "name"],
    right_on = ["tableauTableId", "COLUMN_NAME"]
)

print("final")
join_tableau = join_tableau.drop(["parentTableId", "OBJECT_DATABASE", "OBJECT_SCHEMA", "OBJECT_NAME", "DOMAIN", "COLUMN_NAME", "LEVEL", "TAG_SCHEMA", "TAG_DATABASE"], axis=1)
print(join_tableau)

sensitive_description = "Data that must be protected from unauthorized access to prevent harm to businesses and individuals alike. This classification include personal information, private information, health information, and high-risk data, among others."
for index, row in join_tableau.iterrows():
    update_tableau_column_labels(authToken, pod, siteId, apiVersion, row["id"], row["TAG_VALUE"], sensitive_description)