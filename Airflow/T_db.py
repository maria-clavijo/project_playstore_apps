import pandas as pd
import logging
import loadb

def load_db():
    logging.info("Loading data from MySQL database...")
    data_csv = loadb.use_db()
    logging.info("Data loaded successfully.")
    return data_csv

def m_installs_db(value):
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

def transform_db(**kwargs):
    ti = kwargs["ti"]
    json_data = ti.xcom_pull(task_ids="load_db")
    logging.info("Starting cleaning and transformation processes...")
    df = pd.DataFrame(json_data)

    df['Released'] = pd.to_datetime(df['Released'], format='%b %d, %Y').dt.strftime('%Y-%m-%d')
    df['Last Updated'] = pd.to_datetime(df['Last Updated'], format='%b %d, %Y').dt.strftime('%Y-%m-%d')
    df['Installs'] = df['Installs'].apply(m_installs_db)
    df['Installs'].fillna(0, inplace=True)
    df['Minimum Installs'] = df['Minimum Installs'].apply(m_installs_db)
    df['Minimum Installs'].fillna(0, inplace=True)
    df['Maximum Installs'] = df['Minimum Installs'].apply(m_installs_db)
    df['Maximum Installs'].fillna(0, inplace=True)
    df['App Name'].str.match(r'^[a-zA-Z0-9]+$')
    df['App Name'].fillna('NaN', inplace=True)
    logging.info("Deleted unnecessary columns.")

    df.drop_duplicates(inplace=True)
    logging.info("Removed duplicates.")
    logging.info("Cleaning and transformation processes completed.")
    return df.to_json(orient='records')