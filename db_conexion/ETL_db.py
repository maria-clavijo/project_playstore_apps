import configparser
import pymysql
import pandas as pd

def create_connection():
    config = configparser.ConfigParser()
    config.read('./db_conexion/config.ini')
    host = config['mysql']['host']
    user = config['mysql']['user']
    password = config['mysql']['password']

    try:
        conn = pymysql.connect(
            host=host,
            user=user,
            password=password)
        print("Successful Connection")
        return conn
    except pymysql.Error as e:
        print("Connection Error:", e)
        return None

def create_db(conn):
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS googleplaystoredb")
    cursor.execute("USE googleplaystoredb")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS googleplaystore (
        id INT AUTO_INCREMENT PRIMARY KEY,
        app_name VARCHAR(255) NOT NULL,
        category VARCHAR(255) NOT NULL,
        installs VARCHAR(255) NOT NULL,
        size VARCHAR(255) NOT NULL,
        minimum_android VARCHAR(255) NOT NULL,
        released DATETIME NOT NULL,
        last_updated DATE NOT NULL,
        content_rating VARCHAR(255) NOT NULL,
        rating FLOAT NOT NULL,
        minimum_installs INT NOT NULL,
        maximum_installs VARCHAR(255) NULL
    );
    """)
    conn.commit()
    cursor.close()

def insert_data(conn, df):
    cursor = conn.cursor()
    query = """
    INSERT INTO googleplaystore (app_name, category, installs, size, minimum_android, released, last_updated, content_rating, rating, minimum_installs, maximum_installs)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        for index, row in df.iterrows():
            data = (row["App Name"], row["Category"], row["Installs"], row["Size"],
                    row["Minimum Android"], row["Released"], row["Last Updated"], row["Content Rating"], row["Rating"], row["Minimum Installs"], row["Maximum Installs"])
            cursor.execute(query, data)
        conn.commit()
        print("Data successfully inserted")
    except pymysql.Error as error:
        print("Error when inserting data:", error)
        conn.rollback()
    finally:
        cursor.close()

def max_installs(value):
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

def transform_csv(df):
    df['Released'] = pd.to_datetime(df['Released'], format='%b %d, %Y').dt.strftime('%Y-%m-%d')
    df['Last Updated'] = pd.to_datetime(df['Last Updated'], format='%b %d, %Y').dt.strftime('%Y-%m-%d')
    df['Maximum Installs'] = df['Maximum Installs'].apply(max_installs)
    df['Maximum Installs'].fillna(0, inplace=True)
    df['App Name'].fillna('NaN', inplace=True)
    return df

def main():
    conn = create_connection()
    create_db(conn)
    df_apps = pd.read_csv('./data/Google-Playstore-Dataset-Clean.csv')
    df_transformed = transform_csv(df_apps)
    insert_data(conn, df_transformed)
    conn.close()
    print('The "googleplaystore" table has been successfully created in "googleplaystoredb" database.')

if __name__ == "__main__":
    main()
