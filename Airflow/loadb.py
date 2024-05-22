import configparser
import pymysql
import pandas as pd
import logging
import json
logging.basicConfig(level=logging.INFO)

def create_connection():
    with open('db_conexion/db_settings.json', 'r') as f:
        config = json.load(f)

    try:
        connection = pymysql.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            port=int(config['port'])
        )
        cursor = connection.cursor()
        print("Successful Connection")
        cursor.close()
        return connection
    except pymysql.Error as e:
        print("Connection Error:", e)
        return None


def use_db():
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM googleplaystore")
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        cursor.close()
        connection.close()
        return df


def create_merge_db():
    connection = create_connection()
    cursor = connection.cursor()
    cursor.execute("USE googleplaystoredb")
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS new_googleplaystore (
                id INT AUTO_INCREMENT PRIMARY KEY,
                app_name VARCHAR(255) NOT NULL,
                installs VARCHAR(255) NOT NULL,
                minimum_installs VARCHAR(255) NOT NULL,
                maximum_installs VARCHAR(255) NOT NULL,
                score VARCHAR(255) NOT NULL,
                views VARCHAR(255) NOT NULL,
                category VARCHAR(255) NOT NULL,
                content_rating VARCHAR(255) NOT NULL,
                released DATETIME NOT NULL,
                last_updated DATE NOT NULL);
        """)
    logging.info("Table created successfully")
    cursor.close()
    connection.close()
  

def insert_data_merge_db(json_data):
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        create_merge_db()
        insert_query = """
        INSERT INTO new_googleplaystore (
            app_name, installs, minimum_installs, maximum_installs, score, 
            views, category, content_rating, released, last_updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            if isinstance(json_data, str):
                df = pd.read_json(json_data)
            elif isinstance(json_data, pd.DataFrame):
                df = json_data
            else:
                logging.info("json_data debe ser una cadena JSON válida o un DataFrame.")
            data_tuples = [tuple(row) for row in df.to_records(index=False)]
            cursor.executemany(insert_query, data_tuples)
            connection.commit()
            logging.info("Data inserted successfully")
        except pymysql.Error as e:
            logging.info(f"Error when inserting data:", e)
            connection.rollback()
            cursor.close()
            connection.close()

################################################################
################################################################

def create_dw():
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
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
        connection.commit()
        logging.info("Dimentional tables created successfully")
        cursor.close()
        connection.close()


def insert_dw(data, table):
    connection = create_connection()
    try:
        cursor = connection.cursor()
        column_names = data.columns.tolist()
        place = ", ".join(["%s"] * len(column_names))
        insert_query = f"INSERT INTO {table}({', '.join(column_names)}) VALUES ({place})"
        
        data_tuples = [tuple(row) for row in data.itertuples(index=False)]
        cursor.executemany(insert_query, data_tuples)
        connection.commit()
        logging.info(f"Data successfully inserted in: {table}")
    except pymysql.Error as e:
        logging.error(f"Error inserting data in {table}:", e)
        connection.rollback()
        cursor.close()
        connection.close()

################################################################
################################################################

def create_api_db():
    connection = create_connection()
    cursor = connection.cursor()
    cursor.execute("USE googleplaystoredb")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS api_googleplaystore (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        installs VARCHAR(255) NOT NULL,
        minInstalls BIGINT NOT NULL,
        realInstalls BIGINT NOT NULL,
        score FLOAT NOT NULL,
        ratings BIGINT NOT NULL,
        free VARCHAR(255) NOT NULL,
        genre VARCHAR(255) NOT NULL,
        contentRating VARCHAR(255) NOT NULL,
        released DATE NULL,
        lastUpdatedOn DATE NULL);
    """)
    logging.info("Api table created successfully")
    cursor.close()
    connection.close()


def insert_data_api(json_api):
    connection = create_connection()
    with connection:
        with connection.cursor() as cursor:
            create_api_db() 
            df = pd.read_json(json_api)
            insert_query = """
            INSERT INTO api_googleplaystore (
                title, installs, minInstalls, realInstalls, score, ratings, 
                free, genre, contentRating, released, lastUpdatedOn)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            data_tuples = df[
                ['title', 'installs', 'minInstalls', 'realInstalls', 'score',
                 'ratings', 'free', 'genre', 'contentRating', 'released', 
                 'lastUpdatedOn']
            ].to_records(index=False).tolist()
            cursor.executemany(insert_query, data_tuples)
            connection.commit()
            logging.info("API data inserted successfully")


def use_api():
    df = pd.read_csv("data/datos_api.csv")
    df_api = pd.DataFrame(df)
    return df_api


def query_api_db():
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM api_googleplaystore")
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        cursor.close()
        connection.close()
        return df
    
def use_new_db():
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM new_googleplaystore")
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        cursor.close()
        connection.close()
        return df
    
def insert_new_data(json_data):
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO new_googleplaystore (
            app_name, installs, minimum_installs, maximum_installs, score, 
            views, category, content_rating, released, last_updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        df = pd.DataFrame()
        try:
            if isinstance(json_data, str):
                df = pd.read_json(json_data, orient='records')
            elif isinstance(json_data, pd.DataFrame):
                df = json_data
            if not df.empty:
                data_tuples = [tuple(row) for row in df.to_records(index=False)]
                cursor.executemany(insert_query, data_tuples)
                connection.commit()
                logging.info("Data inserted successfully")
        except Exception as e:
            logging.error(f"Error when inserting data: {e}")
            connection.rollback()
        finally:
            cursor.close()
            connection.close()
