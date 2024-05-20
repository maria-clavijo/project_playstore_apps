import time
import pandas as pd
from loadb import use_db
from services.kafka import kafka_producer
#from services.kafka import kafka_consumer, kafka_producer
#from json import dumps, loads


def stream_data():

    db_sql = '''
    SELECT rating
    FROM new_googleplaystore
    '''
    
    ratings_df = use_db(db_sql)

    for index, row in ratings_df.iterrows():
        message = {'rating': row['rating']}
        kafka_producer(message)
        print(f'Sent message: {message}')
        time.sleep(1)

if __name__ == '__main__':
    stream_data()



""" 
import time
import pandas as pd
import pymysql
from services.kafka import kafka_producer
from loadb import create_connection 

def stream_data():
    connection = create_connection()
    sql = "SELECT rating FROM fact_apps"
    df = pd.read_sql(sql, connection)
    connection.close()
    
    for index, row in df.iterrows():
        kafka_producer(row)
        time.sleep(1)
"""