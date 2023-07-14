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
import snowflake.connector
import logging

def authenticate_snowflake(snow_u,snow_p,account_name,database_name):
    ctx = snowflake.connector.connect(
        user=snow_u,
        password=snow_p,
        account=account_name,
        database=database_name
        )
    cs = ctx.cursor()
    return(cs)

def get_snowflake_tables(cursor_object,database,schema):
    """Query the database, schema, table name, row count and comments from all Tables from a Snowflake Schema. 
    
    Parameters
    ----------
    cursor_object:
        The Snowflake Cursor Object.
    database: str
        Database name to get tables from.
    schema: str
        Schema name to get tables from.   

    Returns
    -------   
    
    """  
    snowflake_tables=cursor_object.execute("select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ROW_COUNT, COMMENT from " + database + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA= '" + schema + "';").fetch_pandas_all()
    return(snowflake_tables)

def get_snowflake__column_metadata(cursor_object, table_name):
    snowflake__column_metadata = cursor_object.execute("select column_name, comment from information_schema.columns where table_name = '"+table_name+"';").fetch_pandas_all()
    return(snowflake__column_metadata)

def get_snowflake_table_column_tags(cursor_object, database, table):
    snowflake_table_column_tags = cursor_object.execute("select * from table("+database+".information_schema.tag_references_all_columns('"+table+"', 'table'));").fetch_pandas_all()
    return(snowflake_table_column_tags)