import time
from loadb import use_db
from services.kafka import kafka_producer
import logging


def stream_data():

    data_sql = "SELECT * FROM new_googleplaystore"
    try:
        df_playstore = use_db(data_sql)
        if df_playstore is None:
            logging.error("The database data could not be accessed.")
            return
        
        logging.info("Initiating transmission process to Kafka...")
        for i, row in df_playstore.iterrows():
            kafka_producer(row)
            time.sleep(1)
        
        logging.info("Transmission process to Kafka completed.")
    except Exception as e:
        logging.error(f"Error in the transmission process to Kafka: {e}")
