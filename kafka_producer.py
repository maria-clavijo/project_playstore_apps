import pandas as pd
import json
import logging
import time
from Airflow.services.kafka import kafka_producer
from Airflow.loadb import use_db, insert_new_data, query_api_db


def extract_db():
    logging.info("Loading data from MySQL database...")
    data_db = use_db()
    logging.info("Data loaded successfully.")
    return data_db

def m_installs(value):
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

def ren_col_api(df):
    column_names = {
        'title': 'app_name',
        'installs': 'installs',
        'minInstalls': 'minimum_installs',
        'realInstalls': 'maximum_installs',
        'score': 'score',
        'ratings': 'views',
        'free': 'free',
        'genre': 'category',
        'contentRating': 'content_rating',
        'released': 'released',
        'lastUpdatedOn': 'last_updated'}
    df.rename(columns=column_names, inplace=True)
    df.columns = [col.lower() for col in df.columns]
    return df[['app_name', 'installs', 'minimum_installs','maximum_installs', 'score', 'views', 'free', 'category', 'content_rating', 'released', 'last_updated']]

def merge(db_df, api_df):
    #ti = kwargs["ti"]
    #db = json.loads(ti.xcom_pull(task_ids="transform_db"))
    #db_df = pd.json_normalize(data=db_df)

    #api = json.loads(ti.xcom_pull(task_ids="transform_api"))
    #api_df = pd.json_normalize(data=api_df)
    logging.info("Data merging process started...")

    db_df = pd.DataFrame(db_df)
    api_df = pd.DataFrame(api_df)
    df3 = ren_col_api(api_df)

    try:
        concat_df = pd.concat([db_df, df3], ignore_index=True)
        columns_of_interest = ['app_name', 'installs', 'minimum_installs', 'maximum_installs', 'score', 'views', 'category', 'content_rating', 'released', 'last_updated']
        merged_data = concat_df[columns_of_interest]
        logging.info("Data merging process successfully completed.")
        return merged_data.to_json(orient='records')
    except Exception as e:
        raise ValueError(f"Error merging data: {e}")

def transform_db(df1):
    #ti = kwargs["ti"]
    #json_data = ti.xcom_pull(task_ids="extract_db")
    logging.info("Starting cleaning and transformation processes...")
    df2 = pd.read_json(df1)
    df = pd.DataFrame(df2)

    df['released'] = pd.to_datetime(df['released'], unit='ms').dt.strftime('%Y-%m-%d')
    df['last_updated'] = pd.to_datetime(df['last_updated'], unit='ms').dt.strftime('%Y-%m-%d')
    default_date = '1900-01-01'
    df['released'].fillna(default_date, inplace=True)
    df['last_updated'].fillna(default_date, inplace=True)
    logging.info("Deleted unnecessary in columns datetime.")

    df['installs'] = df['installs'].apply(m_installs)
    df['installs'].fillna(0, inplace=True)
    df['minimum_installs'] = df['minimum_installs'].apply(m_installs)
    df['minimum_installs'].fillna(0, inplace=True)
    df['maximum_installs'] = df['maximum_installs'].apply(m_installs)
    df['maximum_installs'].fillna(0, inplace=True)
    logging.info("Replace nulls.")

    df['views'] = df['views'].fillna(0)
    df['views'] = df['views'].astype(int)
    logging.info("Convert 'views' to integer.")

    df['app_name'] = df['app_name'].where(df['app_name'].str.match(r'^[a-zA-Z0-9]+$'), 'NaN')
    logging.info("Validate app names and handle missing values")

    df['non_null_count'] = df.notnull().sum(axis=1)
    df = df.sort_values(by=['app_name', 'non_null_count'], ascending=[True, False])
    df = df.drop_duplicates(subset='app_name', keep='first')
    logging.info("Drop duplicates based on 'app_name', keeping the first entry (which has the most non-null values due to sorting")

    df.drop(columns=['non_null_count'], inplace=True)
    logging.info("Deleted unnecessary columns.")

    df.drop_duplicates(inplace=True)
    logging.info("Removed duplicates.")
    logging.info("Cleaning and transformation processes completed.")
    return df

def main():
    df_apps = extract_db()
    api_df = query_api_db()
    df_merged = merge(df_apps, api_df)
    df_transformed = transform_db(df_merged)
    insert_new_data(df_transformed)
    logging.info('The "new_googleplaystore" table has been successfully created.')
    for index, row in df_transformed.iterrows():
        kafka_producer(row)

if __name__ == "__main__":
    main()