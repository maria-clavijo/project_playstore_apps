import logging
import json
import pandas as pd
import loadb
from T_Api import transform_api
from T_db import load_db, transform_db

# COMBINE CSV AND DB DATA
def merge(transform_db, transform_api):
    db = json.loads(transform_db)
    db_df = pd.json_normalize(db)
    api = json.loads(transform_api)
    api_df = pd.json_normalize(api)

    logging.info("Data merging process started...")
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
    loadb.insert_data(data_load)
    logging.info("Data has been successfully loaded into the database.")
