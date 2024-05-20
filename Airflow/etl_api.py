import pandas as pd
import logging
import json
from loadb import use_api, insert_data_api


def extract_api():
    logging.info("Loading data API...")
    data_api = use_api()
    logging.info("Data loaded successfully.")
    json_data = data_api.to_json(orient='records')
    return json_data


def ma_installs(value):
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        value = value.strip().replace(',', '')
        if value.endswith('+'):
            value = value[:-1]

        if value[-1] in 'Kk':
            return int(float(value[:-1]) * 1000)
        elif value[-1] in 'Mm':
            return int(float(value[:-1]) * 1000000)
        elif value[-1] in 'Bb':
            return int(float(value[:-1]) * 1000000000)
    return int(value)


def transform_api(**kwargs):
    ti = kwargs["ti"]
    json_data = ti.xcom_pull(task_ids="read_api")

    if json_data is None:
        raise ValueError("No data available from extract_api task")
    df = pd.json_normalize(json.loads(json_data))
    logging.info("Starting cleaning and transformation processes...")

    df['released'] = pd.to_datetime(df['released'], format='%b %d, %Y').dt.strftime('%Y-%m-%d')
    df['lastUpdatedOn'] = pd.to_datetime(df['lastUpdatedOn'], format='%b %d, %Y').dt.strftime('%Y-%m-%d')
    df['installs'] = df['installs'].apply(ma_installs)
    df['installs'].fillna(0, inplace=True)
    df['minInstalls'] = df['minInstalls'].apply(ma_installs)
    df['minInstalls'].fillna(0, inplace=True)
    df['realInstalls'] = df['realInstalls'].apply(ma_installs)
    df['realInstalls'].fillna(0, inplace=True)
    df['free'] = df['free'].map({True: 1, False: 0})
    df['title'].str.match(r'^[a-zA-Z0-9]+$')
    df['title'].fillna('NaN', inplace=True)
    logging.info("Deleted unnecessary columns.")
    df.isna().all(axis=1)
    df.drop_duplicates(inplace=True)
    logging.info("Removed duplicates and null.")
    logging.info("Cleaning and transformation processes completed.")
    return df.to_json(orient='records')


def load_api(**kwargs):
    logging.info("Starting data loading process...")

    ti = kwargs["ti"]
    apps_api_json = ti.xcom_pull(task_ids="transform_api")

    if apps_api_json is None:
        raise ValueError("No data available from transform_api task")

    insert_data_api(apps_api_json)
    logging.info("Loading completed")
    logging.info("The api_googleplaystore table has been successfully created in googleplaystoredb database.")
