import logging
import json
import pandas as pd
from etl_api import ren_col_api
from loadb import *

# COMBINE CSV AND DB DATA
def merge(db_df, api_df):
    #ti = kwargs["ti"]
    #db = json.loads(ti.xcom_pull(task_ids="transform_db"))
    #db_df = pd.json_normalize(data=db)

    #api = json.loads(ti.xcom_pull(task_ids="transform_api"))
    #api_df = pd.json_normalize(data=api)
    #logging.info("Data merging process started...")
    
    api_df = ren_col_api(api_df)
    logging.info("Rename columns.")
    
    df_list = [db_df, api_df]
    columns_set = {tuple(df.columns) for df in df_list}
    if len(columns_set) != 1:
        raise ValueError("All DataFrames must have the same columns to concatenate. Please check the column names.")
    concat_df = pd.concat(df_list, ignore_index=True)
    col_interest = ['app_name', 'installs', 'minimum_installs','maximum_installs', 'score', 'views', 'category', 'content_rating', 'released', 'last_updated']
    merged_data = concat_df[col_interest]
    logging.info("Data merging process successfully completed.")
    return merged_data

# LOAD DATA TO DATABASE
def load(**kwargs):
    ti = kwargs["ti"]
    data = json.loads(ti.xcom_pull(task_ids="merge"))
    data_load = pd.json_normalize(data=data)
    new_data = loadb.insert_data_merge_db(data_load)
    logging.info("Data has been successfully loaded into the database.")