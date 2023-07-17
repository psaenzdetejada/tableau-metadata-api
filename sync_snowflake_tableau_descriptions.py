# Import the required python modules
import requests
import json
import pandas as pd
import snowflake.connector

from tableau_api_functions import authenticate_tableau, get_tableau_tables_external_assets, update_tableau_table_description, get_tableau_columns_from_table, update_tableau_columns_descriptions
from snowflake_functions import authenticate_snowflake, get_snowflake_tables, get_snowflake__column_metadata

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
snow_tables = get_snowflake_tables(auth_snow, "PERSONAL", "PUBLIC")
column_rename = {"TABLE_CATALOG": "snowflakeTableDatabase", "TABLE_SCHEMA": "snowflakeTableSchema", "TABLE_NAME": "snowflakeTableName", "ROW_COUNT": "snowflakeTableRowCount", "COMMENT": "snowflakeTableComment"}
snow_tables = snow_tables.rename(columns=column_rename)
print(snow_tables)

# Join Tableau and Snowflake Tables
df_snow_tableau_tables = pd.merge(
    external_assets,
    snow_tables,
    how="inner",
    left_on=["tableauTableName", "tableauTableSchema"],
    right_on=["snowflakeTableName", "snowflakeTableSchema"]
)

print("\nResult of joining Tableau External Assets and Snowflake Tables:\n")
print(df_snow_tableau_tables)

df_tableau_columns = pd.DataFrame()
df_snowflake_columns = pd.DataFrame()

for index, row in df_snow_tableau_tables.iterrows():
    # update_tableau_table_description(authToken, pod, siteId, row["tableauTableId"], "3.20", row["snowflakeTableComment"], row["snowflakeTableRowCount"])
    tableau_columns = get_tableau_columns_from_table(authToken, pod, siteId, row["tableauTableId"], "3.20")
    df_tableau_columns = pd.concat([tableau_columns, df_tableau_columns])
    snowflakle_columns = get_snowflake__column_metadata(auth_snow, row["snowflakeTableDatabase"], row["snowflakeTableSchema"], row["snowflakeTableName"])
    df_snowflake_columns = pd.concat([snowflakle_columns, df_snowflake_columns])

# print(df_tableau_columns)
df_tableau_columns = df_tableau_columns.drop(["site", "tags", "description", "remoteType", "nullable"], axis=1)
column_rename = {"id": "tableauColumnId", "name": "tableauColumnName", "parentTableId": "tableauTableId"}
df_tableau_columns = df_tableau_columns.rename(columns=column_rename)
# print(df_tableau_columns)

df_columns = pd.merge(
    df_tableau_columns,
    external_assets,
    how = "inner",
    left_on = "tableauTableId",
    right_on = "tableauTableId"
)
# print(df_columns)

df_columns_complete = pd.merge(
    df_columns, 
    df_snowflake_columns,
    how = "inner",
    left_on = ["tableauTableSchema", "tableauTableName", "tableauColumnName"],
    right_on = ["TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME"]
)

print(df_columns_complete)

for index, row in df_columns_complete.iterrows():
    update_tableau_columns_descriptions(authToken, pod, siteId, row["tableauTableId"], row["tableauColumnId"], "3.20", row["COMMENT"])
