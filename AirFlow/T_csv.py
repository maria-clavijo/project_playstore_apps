import pandas as pd
import logging
import calldb

def load_db():
    logging.info("Loading data from MySQL database...")
    data_csv = calldb.query_db()
    logging.info("Data loaded successfully.")
    return data_csv


def transform_db(**kwargs):
    ti = kwargs["ti"]
    json_data = ti.xcom_pull(task_ids="load_db")
    logging.info("Starting cleaning and transformation processes...")
    df = pd.DataFrame(json_data)

    df.dropna(subset=['artist'], inplace=True)
    logging.info("Handled missing values.")

    df[['winner', 'nominee', 'artist', 'year']]
    df.rename(columns={'winner': 'was_nominated'}, inplace=True)
    logging.info("Deleted unnecessary columns.")

    df.drop_duplicates(inplace=True)
    logging.info("Removed duplicates.")
    logging.info("Cleaning and transformation processes completed.")
    return df.to_json(orient='records')

def max_installs_Api(value):
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