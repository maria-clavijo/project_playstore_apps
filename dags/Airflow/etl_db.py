import pandas as pd
import logging
from loadb import use_db, insert_data_merge_db, query_api_db

def extract_db():
    logging.info("Loading data from MySQL database...")
    data_db = use_db()
    logging.info("Data loaded successfully.")
    return data_db

def extract_api():
    logging.info("Loading data api from MySQL database...")
    data_api = query_api_db()
    logging.info("Data api loaded successfully.")
    return data_api

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
    #db_df = pd.json_normalize(data=db)

    #api = json.loads(ti.xcom_pull(task_ids="transform_api"))
    #api_df = pd.json_normalize(data=api)
    #logging.info("Data merging process started...")

    db_df = pd.read_json(db_df)
    api_df = pd.read_json(api_df)
    df3 = ren_col_api(api_df)

    try:
        concat_df = pd.concat([db_df, df3], ignore_index=True)
        columns_of_interest = ['app_name', 'installs', 'minimum_installs', 'maximum_installs', 'score', 'views', 'category', 'content_rating', 'released', 'last_updated']
        merged_data = concat_df[columns_of_interest]
        logging.info("Data merging process successfully completed.")
        return merged_data.to_json(orient='records')
    except Exception as e:
        raise ValueError(f"Error merging data: {e}")
    
def transform_db(json_data):
    #ti = kwargs["ti"]
    #json_data = ti.xcom_pull(task_ids="extract_db")
    #logging.info("Starting cleaning and transformation processes...")
    df1 = pd.read_json(json_data)
    df = pd.DataFrame(df1)

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
    return df.to_json(orient='records')


def load_new(df):
    logging.info("Starting data loading process...")
       
    #ti = kwargs["ti"]
    #apps_api_df = pd.json_normalize(json.loads(ti.xcom_pull(task_ids="transform_db")))
    apps_api_df = df
    insert_data_merge_db(apps_api_df)
    logging.info("The new_googleplaystore table has been successfully created in googleplaystoredb database.")

#def main():
#    df_apps = extract_db()
#    api_df = query_api_db()
#    df_merged = merge(df_apps, api_df)
#    df_transformed = transform_db(df_merged)
#    load_new(df_transformed)
#    print('The "new_googleplaystore" table has been successfully created in "googleplaystoredb" database.')
#    print(df_transformed)

#if __name__ == "__main__":
#    main()