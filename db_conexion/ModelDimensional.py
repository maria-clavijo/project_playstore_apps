import configparser
import pymysql
import pandas as pd


def create_connection():
    config = configparser.ConfigParser()
    config.read('config.ini')
    host = config['mysql']['host']
    user = config['mysql']['user']
    password = config['mysql']['password']

    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password)
        print("Successful Connection")
        return connection
    except pymysql.Error as e:
        print("Connection Error:", e)
        return None



def create_dimension_tables(connection):
    """Crea las tablas de dimensiones en la base de datos."""
    try:
        cursor = connection.cursor()

        # Crear la base de datos si no existe
        cursor.execute("CREATE DATABASE IF NOT EXISTS mdplaystore_apps")
        cursor.execute("USE mdplaystore_apps")
        

        # Tabla Dim_App
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_app (
                app_id INT AUTO_INCREMENT PRIMARY KEY,
                app_name VARCHAR(255) NOT NULL,
                content_rating VARCHAR(255) NOT NULL
            )
        """)

        # Tabla Dim_Category
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_category (             
                category_id INT AUTO_INCREMENT PRIMARY KEY,
                category_name VARCHAR(255) NOT NULL
            )
        """)

        # Tabla Dim_Android
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_android (
                android_id INT AUTO_INCREMENT PRIMARY KEY,
                size VARCHAR(255) NOT NULL,
                minimum_android VARCHAR(255) NOT NULL
            )
        """)

        # Tabla Dim_Released
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_released (
                released_ID INT AUTO_INCREMENT PRIMARY KEY,
                year INT NOT NULL,
                month INT NOT NULL,
                day INT NOT NULL
            )
        """)

        # Tabla Dim_Last_Updated
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_last_updated (
                last_updated_id INT AUTO_INCREMENT PRIMARY KEY,
                year_updated INT NOT NULL,
                month_updated INT NOT NULL,
                day_updated INT NOT NULL
            )
        """)

        connection.commit()
        print("Dimension tables created successfully")
    except pymysql.Error as e:
        print("Error creating dimension tables:", e)



def create_fact_table(connection):
    """Crea la tabla de hechos (fact table) en la base de datos."""
    try:
        cursor = connection.cursor()
        cursor.execute("USE mdplaystore_apps")

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
                FOREIGN KEY (android_id) REFERENCES dim_android(android_id)
            )
        """)

        connection.commit()
        print("Fact table 'fact_apps' created successfully")
    except pymysql.Error as e:
        print("Error creating fact table:", e)



def extract_data_from_db(connection):
    """Extrae datos de la fuente (googleplaystoredb)."""
    try:
        cursor = connection.cursor()
        cursor.execute("USE googleplaystoredb")

        # Consulta para extraer datos de googleplaystoredb
        query = """
            SELECT app_name, category, rating, installs, minimum_installs, maximum_installs,
                   size, minimum_android, released, last_updated, content_rating
            FROM googleplaystore
        """

        # Ejecutar la consulta
        cursor.execute(query)

        # Obtener todos los resultados
        data = cursor.fetchall()

        return data

    except pymysql.Error as e:
        print("Error al extraer datos:", e)
        return None


def transform_data_for_dimension_tables(data):
    """Transforma los datos para las tablas de dimensiones."""
    transformed_data = []
    for row in data:
        # Extraer año, mes y día de la columna Released
        released_date = row['released']
        released_year = released_date.year
        released_month = released_date.month
        released_day = released_date.day
        
        # Extraer año, mes y día de la columna Last Updated
        updated_date = row['last_updated']
        updated_year = updated_date.year
        updated_month = updated_date.month
        updated_day = updated_date.day

        
        transformed_row = (
            row['app_name'],
            row['category'],
            row['rating'],
            row['size'],
            row['minimum_android'],
            released_year,
            released_month,
            released_day,
            updated_year,
            updated_month,
            updated_day,
            row['content_rating']
        )
        transformed_data.append(transformed_row)

    return transformed_data


def transform_data_for_fact_table(data):
    """Transforma los datos para la tabla de hechos."""
    transformed_data = []
    for row in data:
        # Convertir installs de VARCHAR a BIGINT (si es posible)
        installs = int(row['installs'].replace('+', '').replace(',', ''))

        # Convertir maximum_installs de VARCHAR a BIGINT (si es posible)
        maximum_installs = int(row['maximum_installs'].replace('+', '').replace(',', '')) if row['maximum_installs'] else None

        # Aquí puedes realizar cualquier otra transformación necesaria
        transformed_row = (
            row['app_name'],
            row['category'],
            row['rating'],
            installs,
            row['minimum_installs'],
            maximum_installs,
            row['size'],
            row['minimum_android'],
            row['released'],  # Dejar la fecha original para la tabla de hechos
            row['last_updated'],  # Dejar la fecha original para la tabla de hechos
            row['content_rating']
        )
        transformed_data.append(transformed_row)

    return transformed_data


def insert_into_dim_app(connection, data):
    """Inserta datos en la tabla dim_app."""
    try:
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO dim_app (app_name, content_rating)
            VALUES (%s, %s)
        """
        cursor.executemany(insert_query, data)
        connection.commit()
        print("Data inserted successfully into dim_app table")
    except pymysql.Error as e:
        print("Error inserting data into dim_app table:", e)


def insert_into_dim_category(connection, data):
    """Inserta datos en la tabla dim_category."""
    try:
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO dim_category (category_name)
            VALUES (%s)
        """
        cursor.executemany(insert_query, data)
        connection.commit()
        print("Data inserted successfully into dim_category table")
    except pymysql.Error as e:
        print("Error inserting data into dim_category table:", e)


def insert_into_dim_android(connection, data):
    """Inserta datos en la tabla dim_android."""
    try:
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO dim_android (size, minimum_android)
            VALUES (%s, %s)
        """
        cursor.executemany(insert_query, data)
        connection.commit()
        print("Data inserted successfully into dim_android table")
    except pymysql.Error as e:
        print("Error inserting data into dim_android table:", e)


def insert_into_dim_released(connection, data):
    """Inserta datos en la tabla dim_released."""
    try:
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO dim_released (year, month, day)
            VALUES (%s, %s, %s)
        """
        cursor.executemany(insert_query, data)
        connection.commit()
        print("Data inserted successfully into dim_released table")
    except pymysql.Error as e:
        print("Error inserting data into dim_released table:", e)


def insert_into_dim_last_updated(connection, data):
    """Inserta datos en la tabla dim_last_updated."""
    try:
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO dim_last_updated (year_updated, month_updated, day_updated)
            VALUES (%s, %s, %s)
        """
        cursor.executemany(insert_query, data)
        connection.commit()
        print("Data inserted successfully into dim_last_updated table")
    except pymysql.Error as e:
        print("Error inserting data into dim_last_updated table:", e)


def insert_into_fact_table(connection, data):
    """Inserta datos en la tabla de hechos (fact_apps)."""
    try:
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO fact_apps (rating, installs, minimum_installs, maximum_installs, app_id, 
            released_id, last_updated_id, category_id, android_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, data)
        connection.commit()
        print("Data inserted successfully into fact_apps table")
    except pymysql.Error as e:
        print("Error inserting data into fact_apps table:", e)



if __name__ == "__main__":
    try:
        # Conectarse a la base de datos fuente (googleplaystoredb)
        source_connection = create_connection()
        
        # Conectarse a la base de datos de destino (mdplaystore_apps)
        destination_connection = create_connection()

        if source_connection and destination_connection:
            # Crear las tablas de dimensiones y tabla de hechos en la base de datos de destino
            create_dimension_tables(destination_connection)
            create_fact_table(destination_connection)

            # Extraer datos de la base de datos fuente
            data_to_load = extract_data_from_db(source_connection)

            if data_to_load:
                try:
                    # Transformar y cargar datos en las tablas de dimensiones
                    transformed_dimension_data = transform_data_for_dimension_tables(data_to_load)
                    insert_into_dim_app(destination_connection, [(row[0], row[1]) for row in transformed_dimension_data])
                    insert_into_dim_category(destination_connection, [(row[1],) for row in transformed_dimension_data])
                    insert_into_dim_android(destination_connection, [(row[6], row[7]) for row in transformed_dimension_data])
                    insert_into_dim_released(destination_connection, [(row[8], row[9], row[10]) for row in transformed_dimension_data])
                    insert_into_dim_last_updated(destination_connection, [(row[11], row[12], row[13]) for row in transformed_dimension_data])

                    # Transformar y cargar datos en la tabla de hechos (fact table)
                    transformed_fact_data = transform_data_for_fact_table(data_to_load)
                    insert_into_fact_table(destination_connection, [(row[2], row[3], row[4], row[5], app_id, released_id, last_updated_id, category_id, android_id) for (app_id, released_id, last_updated_id, category_id, android_id), row in enumerate(transformed_fact_data)])
                    
                    print("Data loaded successfully into dimensional tables and fact table")
                except Exception as e:
                    print("Error loading data:", e)
            else:
                print("No data to load from source")

        else:
            print("Error connecting to one of the databases")

    finally:
        # Cerrar conexiones
        if source_connection:
            source_connection.close()
        if destination_connection:
            destination_connection.close()
