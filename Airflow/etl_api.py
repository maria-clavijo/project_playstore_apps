import pandas as pd
import logging
import json
from Airflow.loadb import use_api, insert_data_api

def extract_api():
    logging.info("Loading data from MySQL database...")
    data_api = use_api()
    logging.info("Data loaded successfully.")
    return data_api

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
    apps_df = ti.xcom_pull(task_ids="extract_api")
    #apps_df = df
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
    apps_api_df = pd.json_normalize(json.loads(ti.xcom_pull(task_ids="transform_api")))
    #apps_api_df = df
    insert_data_api(apps_api_df)
    logging.info("Loading completed")
    logging.info("The api_googleplaystore table has been successfully created in googleplaystoredb database.")

#def main():
#    df_apps = extract_api()
#    df_transformed = transform_api(df_apps)
#    load_api(df_transformed)
#    print('The "api_googleplaystore" table has been successfully created in "googleplaystoredb" database.')

#if __name__ == "__main__":
#    main()