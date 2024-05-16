import pandas as pd
import logging
import json
from loadb import create_api_db, use_api_db, insert_data_api

def extract_api():
    logging.info("Loading data from MySQL database...")
    data_api = use_api_db()
    logging.info("Data loaded successfully.")
    return data_api

def ma_installs(value):
    if isinstance(value, int):
        return value

    if isinstance(value, str):
        value = value.strip()
        if value.endswith('+'):
            value = value[:-1]

        if value[-1] in 'Kk':
            return int(float(value[:-1]) * 1000)
        elif value[-1] in 'Mm':
            return int(float(value[:-1]) * 1000000)
        elif value[-1] in 'Bb':
            return int(float(value[:-1]) * 1000000000)
    return int(value)

def ren_col_api(df):
    column_names = {
        'title': 'app_name',
        'installs': 'installs',
        'minInstalls': 'minimum_installs',
        'realInstalls': 'maximum_installs',
        'score': 'score',
        'rating': 'views',
        'genre': 'category',
        'contentRating': 'content_rating',
        'released': 'released',
        'lastUpdatedOn': 'last_updated'}
    df.rename(columns=column_names, inplace=True)
    df.columns = [col.lower() for col in df.columns]
    return df[['app_name', 'installs', 'minimum_installs','maximum_installs', 'score', 'views', 'category', 'content_rating', 'released', 'last_updated']]

def transform_api(**kwargs):
    ti = kwargs["ti"]
    apps_df = ti.xcom_pull(task_ids="load_api")
    logging.info("Starting cleaning and transformation processes...")
    df = pd.DataFrame(apps_df)

    df['released'] = pd.to_datetime(df['released'], format='%b %d, %Y').dt.strftime('%Y-%m-%d')
    df['lastUpdatedOn'] = pd.to_datetime(df['lastUpdatedOn'], format='%b %d, %Y').dt.strftime('%Y-%m-%d')
    df['installs'] = df['installs'].apply(ma_installs)
    df['installs'].fillna(0, inplace=True)
    df['minInstalls'] = df['minInstalls'].apply(ma_installs)
    df['minInstalls'].fillna(0, inplace=True)
    df['realInstalls'] = df['realInstalls'].apply(ma_installs)
    df['realInstalls'].fillna(0, inplace=True)
    df['title'].str.match(r'^[a-zA-Z0-9]+$')
    df['title'].fillna('NaN', inplace=True)
    logging.info("Deleted unnecessary columns.")

    df1 = ren_col_api(df)
    logging.info("Rename columns.")

    df1.drop_duplicates(inplace=True)
    logging.info("Removed duplicates.")
    logging.info("Cleaning and transformation processes completed.")
    return df1.to_json(orient='records')

def load_api(**kwargs):
    logging.info("Starting data loading process...")
    create_api_db()

    ti = kwargs["ti"]
    apps_api_df = pd.json_normalize(json.loads(ti.xcom_pull(task_ids="transform_api")))

    insert_data_api(apps_api_df)
    logging.info("Loading completed")