import pandas as pd
from loadb import create_connection

def use_db():
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

df = use_db()
print(df.head())
print(df.info())
    

