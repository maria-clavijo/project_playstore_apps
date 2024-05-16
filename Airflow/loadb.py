import configparser
import mysql.connector
import numpy as np
import pandas as pd
import logging

def create_connection():
    config = configparser.ConfigParser()
    config.read('./db_conexion/config.ini')
    host = config['mysql']['host']
    user = config['mysql']['user']
    password = config['mysql']['password']
    database = config['mysql']['database']
    
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            print("Successful Connection")
            return connection
    except mysql.connector.Error as e:
        print("Connection Error:", e)
        return None

def use_db():
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        db_table = 'SELECT * FROM googleplaystore'
        cursor.execute(db_table)
        rows = cursor.fetchall()
        columns = cursor.column_names
        df = pd.DataFrame(rows, columns=columns)
        connection.close()
        return df.to_json(orient='records')
    
def create_db(cursor):
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
    logging.info("Table created successfully")
    cursor.commit()
    cursor.close()

def insert_data(json_data):
    connection = create_connection()
    if connection is not None:
        df = pd.read_json(json_data)
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO googleplaystore (app_name, category, installs, size, minimum_android, released, last_updated, content_rating, rating, minimum_installs, maximum_installs)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            data_tuples = [tuple(x) for x in df.to_numpy()]
            cursor.executemany(insert_query, data_tuples)
            connection.commit()
            logging.info("Data inserted successfully")
        except mysql.connector.Error as e:
            logging.info("Failed to insert data: %s", e)
            connection.rollback()
        finally:
            cursor.close()
            connection.close()

################################################################
################################################################
def create_dw(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("USE googleplaystoredb")
        tables = {
            "dim_app": """
                CREATE TABLE IF NOT EXISTS dim_app (
                    app_id INT AUTO_INCREMENT PRIMARY KEY,
                    app_name VARCHAR(255) NOT NULL,
                    content_rating VARCHAR(255) NOT NULL)""",
            
            "dim_category": """
                CREATE TABLE IF NOT EXISTS dim_category (
                    category_id INT AUTO_INCREMENT PRIMARY KEY,
                    category_name VARCHAR(255) NOT NULL)""",
            
            "dim_android": """
                CREATE TABLE IF NOT EXISTS dim_android (
                    android_id INT AUTO_INCREMENT PRIMARY KEY,
                    size VARCHAR(255) NOT NULL,
                    minimum_android VARCHAR(255) NOT NULL)""",
            
            "dim_released": """
                CREATE TABLE IF NOT EXISTS dim_released (
                    released_id INT AUTO_INCREMENT PRIMARY KEY,
                    year INT NOT NULL,
                    month INT NOT NULL,
                    day INT NOT NULL)""",
            
            "dim_last_updated": """
                CREATE TABLE IF NOT EXISTS dim_last_updated (
                    last_updated_id INT AUTO_INCREMENT PRIMARY KEY,
                    year_updated INT NOT NULL,
                    month_updated INT NOT NULL,
                    day_updated INT NOT NULL)""",

            "fact_apps": """
                CREATE TABLE IF NOT EXISTS fact_apps (
                    fact_id INT AUTO_INCREMENT PRIMARY KEY,
                    rating FLOAT,
                    installs BIGINT,
                    minimum_installs BIGINT,
                    maximum_installs BIGINT,
                    app_id INT,
                    released_id INT,
                    last_updated_id INT,
                    category_id INT,
                    android_id INT,
                    FOREIGN KEY (app_id) REFERENCES dim_app(app_id),
                    FOREIGN KEY (released_id) REFERENCES dim_released(released_id),
                    FOREIGN KEY (last_updated_id) REFERENCES dim_last_updated(last_updated_id),
                    FOREIGN KEY (category_id) REFERENCES dim_category(category_id),
                    FOREIGN KEY (android_id) REFERENCES dim_android(android_id))"""

        }
        
        for table, query in tables.items():
            cursor.execute(query)
        conn.commit()
        print(f"Dimension {table} created successfully")
    except Exception as e:
        print(f"Error creating dimension tables of {table}:", e)

def insert_dw(conn, data, table):
    with conn.cursor() as cursor:
        column_names = data.columns.tolist()
        insert_query = f"""
            INSERT INTO {table}({", ".join(column_names)})
            VALUES ({", ".join(["%s"] * len(column_names))})
        """
    try:
        cnx = create_connection()
        cur = cnx.cursor()
        for index, row in data.iterrows():
            values = tuple(row)
            cur.execute(insert_query, values)
        cur.close()
        cnx.commit()
        print(f"Data inserted successfully into: {table}")
    except Exception as e:
        print(f"Error inserting data into {table}:", e)

################################################################
################################################################
def create_api_db(cursor):
    cursor.execute("USE googleplaystoredb")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS apigoogleplaystore (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        installs VARCHAR(255) NOT NULL,
        minInstalls VARCHAR(255) NOT NULL,
        realInstalls VARCHAR(255) NOT NULL,
        score VARCHAR(255) NOT NULL,
        ratings DATETIME NOT NULL,
        free DATE NOT NULL,
        genre VARCHAR(255) NOT NULL,
        rating FLOAT NOT NULL,
        contentRating INT NOT NULL,
        released VARCHAR(255) NULL,
        lastUpdatedOn VARCHAR(255) NULL,           
    );
    """)
    logging.info("Table created successfully")
    cursor.commit()
    cursor.close()

def use_api_db():
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        db_table = 'SELECT * FROM apigoogleplaystore'
        cursor.execute(db_table)
        rows = cursor.fetchall()
        columns = cursor.column_names
        df = pd.DataFrame(rows, columns=columns)
        connection.close()
        return df.to_json(orient='records')
    


def insert_data_api(json_data):
    connection = create_connection()
    if connection is not None:
        df = pd.read_json(json_data)
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO apigoogleplaystore (title, installs, minInstalls, realInstalls, score, ratings, free, genre, contentRating, released, lastUpdatedOn)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            data_tuples = [tuple(x) for x in df.to_numpy()]
            cursor.executemany(insert_query, data_tuples)
            connection.commit()
            logging.info("Data inserted successfully")
        except mysql.connector.Error as e:
            logging.info("Failed to insert data: %s", e)
            connection.rollback()
        finally:
            cursor.close()
            connection.close()