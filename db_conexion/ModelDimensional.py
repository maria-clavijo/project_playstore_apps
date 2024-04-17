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
        connection = pymysql.connect(host=host, 
                                     user=user, 
                                     password=password)
        print("Successful Connection")
        return connection
    except pymysql.Error as e:
        print("Connection Error:", e)
        return None

def create_dim_tables(conn):
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
                    day_updated INT NOT NULL)"""
        }
        for table, query in tables.items():
            cursor.execute(query)
        conn.commit()
        print(f"Dimension {table} created successfully")
    except pymysql.Error as e:
        print(f"Error creating dimension tables of {table}:", e)

def create_fact_table(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("USE googleplaystoredb")
        cursor.execute("""
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
            FOREIGN KEY (android_id) REFERENCES dim_android(android_id))
        """)
        conn.commit()
        print("Fact table 'fact_apps' created successfully")
    except pymysql.Error as e:
        print("Error creating fact table:", e)

#if __name__ == "__main__":
#    try:
#        connection = create_connection()
#        if connection:
#            create_dimension_tables(connection)
#            create_fact_table(connection)
#            # Agregar la lógica para extraer, transformar e insertar datos
#            print("Operation completed successfully")
#    except Exception as e:
#        print("An error occurred:", e)
#    finally:
#        if connection:
#            connection.close()

def query_db(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("USE googleplaystoredb")
        query = """
                SELECT * FROM googleplaystore
        """
        cursor.execute(query)
        df = pd.read_sql_query(query, conn)
        return df
    except pymysql.Error as e:
        print("Error al extraer datos:", e)
        return None

################################################
def transform_dm_tables(data):
    transformed_data = []
    for index, row in data.iterrows():
        # Extraer año, mes y día de la columna Released
        released_date = pd.to_datetime(row['released'])  # Asegurarse de que es un tipo datetime
        released_year = released_date.year
        released_month = released_date.month
        released_day = released_date.day
        
        # Extraer año, mes y día de la columna Last Updated
        updated_date = pd.to_datetime(row['last_updated'])  # Asegurarse de que es un tipo datetime
        updated_year = updated_date.year
        updated_month = updated_date.month
        updated_day = updated_date.day

        transformed_row = {
            'app_name': row['app_name'],
            'category': row['category'],
            'rating': row['rating'],
            'size': row['size'],
            'minimum_android': row['minimum_android'],
            'released_year': released_year,
            'released_month': released_month,
            'released_day': released_day,
            'updated_year': updated_year,
            'updated_month': updated_month,
            'updated_day': updated_day,
            'content_rating': row['content_rating']
        }
        transformed_data.append(transformed_row)
    return pd.DataFrame(transformed_data)

def transform_fact_table(data):
    transformed_data = []
    for index, row in data.iterrows():
        installs = int(row['installs'].replace('+', '').replace(',', ''))
        # Asumiendo que 'maximum_installs' es un campo que ya existe en el DataFrame
        maximum_installs = row['maximum_installs']

        transformed_row = {
            'app_name': row['app_name'],
            'category': row['category'],
            'rating': row['rating'],
            'installs': installs,
            'minimum_installs': row['minimum_installs'],
            'maximum_installs': maximum_installs,
            'size': row['size'],
            'minimum_android': row['minimum_android'],
            'released': row['released'],  # Dejar la fecha original para la tabla de hechos
            'last_updated': row['last_updated'],  # Dejar la fecha original para la tabla de hechos
            'content_rating': row['content_rating']
        }
        transformed_data.append(transformed_row)
    return pd.DataFrame(transformed_data)

################################################
def insert_dim_app(conn, data):
    try:
        with conn.cursor() as cursor:
            insert_query = """
                INSERT INTO dim_app (app_name, content_rating)
                VALUES (%s, %s)
            """
            cursor.executemany(insert_query, data)
            conn.commit()
            print("Data inserted successfully into dim_app table")
    except pymysql.Error as e:
        print("Error inserting data into dim_app table:", e)

def insert_dim_category(conn, data):
    try:
        with conn.cursor() as cursor:
            insert_query = """
                INSERT INTO dim_category (category_name)
                VALUES (%s)
            """
            cursor.executemany(insert_query, data)
            conn.commit()
            print("Data inserted successfully into dim_category table")
    except pymysql.Error as e:
        print("Error inserting data into dim_category table:", e)

def insert_dim_android(conn, data):
    try:
        with conn.cursor() as cursor:
            insert_query = """
                INSERT INTO dim_android (size, minimum_android)
                VALUES (%s, %s)
            """
            cursor.executemany(insert_query, data)
            conn.commit()
            print("Data inserted successfully into dim_android table")
    except pymysql.Error as e:
        print("Error inserting data into dim_android table:", e)

def insert_dim_released(conn, data):
    try:
        with conn.cursor() as cursor:
            insert_query = """
                INSERT INTO dim_released (year, month, day)
                VALUES (%s, %s, %s)
            """
            cursor.executemany(insert_query, data)
            conn.commit()
            print("Data inserted successfully into dim_released table")
    except pymysql.Error as e:
        print("Error inserting data into dim_released table:", e)

def insert_dim_last_updated(conn, data):
    try:
        with conn.cursor() as cursor:
            insert_query = """
                INSERT INTO dim_last_updated (year_updated, month_updated, day_updated)
                VALUES (%s, %s, %s)
            """
            cursor.executemany(insert_query, data)
            conn.commit()
            print("Data inserted successfully into dim_last_updated table")
    except pymysql.Error as e:
        print("Error inserting data into dim_last_updated table:", e)

def insert_fact_table(conn, data):
    try:
        with conn.cursor() as cursor:
            insert_query = """
                INSERT INTO fact_apps (rating, installs, minimum_installs, maximum_installs)
                VALUES (%s, %s, %s, %s)
            """
            cursor.executemany(insert_query, data)
            conn.commit()
            print("Data inserted successfully into fact_apps table")
    except pymysql.Error as e:
        print("Error inserting data into fact_apps table:", e)

def foreign_keys(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("USE googleplaystoredb")
        foreign_keys = {
            "fk_app_id": """
                ALTER TABLE fact_apps ADD CONSTRAINT fk_app_id
                FOREIGN KEY (app_id) REFERENCES dim_app(app_id)
            """,
            "fk_released_id": """
                ALTER TABLE fact_apps ADD CONSTRAINT fk_released_id
                FOREIGN KEY (released_id) REFERENCES dim_released(released_id)
            """,
            "fk_last_updated_id": """
                ALTER TABLE fact_apps ADD CONSTRAINT fk_last_updated_id
                FOREIGN KEY (last_updated_id) REFERENCES dim_last_updated(last_updated_id)
            """,
            "fk_category_id": """
                ALTER TABLE fact_apps ADD CONSTRAINT fk_category_id
                FOREIGN KEY (category_id) REFERENCES dim_category(category_id)
            """,
            "fk_android_id": """
                ALTER TABLE fact_apps ADD CONSTRAINT fk_android_id
                FOREIGN KEY (android_id) REFERENCES dim_android(android_id)
            """
        }
        for key, query in foreign_keys.items():
            cursor.execute(query)
        conn.commit()
        print("Foreign keys added successfully to fact_apps table")
    except pymysql.Error as e:
        print("Error adding foreign keys to fact_apps table:", e)

def main():
    conn = create_connection()
    
    # Crear tablas dimensionales y tabla de hechos si no existen
    create_dim_tables(conn)
    create_fact_table(conn)
    
    df_apps = query_db(conn)
    
    # Transformar datos
    df_transformed_dims = transform_dm_tables(df_apps)
    df_transformed_fact = transform_fact_table(df_apps)
    
    # Insertar datos en las tablas dimensionales
    insert_dim_app(conn, [(row['app_name'], row['content_rating']) for index, row in df_transformed_dims.iterrows()])
    insert_dim_category(conn, [(row['category'],) for index, row in df_transformed_dims.iterrows()])
    insert_dim_android(conn, [(row['size'], row['minimum_android']) for index, row in df_transformed_dims.iterrows()])
    insert_dim_released(conn, [(row['released_year'], row['released_month'], row['released_day']) for index, row in df_transformed_dims.iterrows()])
    insert_dim_last_updated(conn, [(row['updated_year'], row['updated_month'], row['updated_day']) for index, row in df_transformed_dims.iterrows()])

    # Transformar y cargar datos en la tabla de hechos (fact table)
    insert_fact_table(conn, [(row['rating'], row['installs'], row['minimum_installs'], row['maximum_installs']) for index, row in df_transformed_fact.iterrows()])
    foreign_keys(conn)
    conn.close()
    print('The dimensional tables and the fact table have been successfully created in the database')

if __name__ == "__main__":
    main()