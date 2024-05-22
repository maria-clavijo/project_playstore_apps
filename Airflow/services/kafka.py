from json import  dumps, loads
import json
from kafka import KafkaProducer, KafkaConsumer
import pandas as pd
import requests
import logging
from Airflow.loadb import insert_new_data

def kafka_producer(row):
    producer = KafkaProducer(
        value_serializer=lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers=['localhost:9092'],
    )

    message = row.to_dict()
    producer.send('kafka-playstore-apps', value=message)
    print("Message sent")

def kafka_consumer():
    
    API_ENDPOINT = 'https://api.powerbi.com/beta/YOUR_POWERBI_WORKSPACE_ID/datasets/YOUR_DATASET_ID/rows?key=YOUR_API_KEY'
    
    consumer = KafkaConsumer(
        'kafka-playstore-apps',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-1',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        bootstrap_servers=['localhost:9092']
    )

    for message in consumer:
        df = pd.json_normalize(data=message.value)
        if not df.empty:
            first_record = df.iloc[0]
            insert_new_data(first_record.to_dict())
            logging.info("Data inserted into database MySQL:\n%s", df)
            data = df.to_json(orient='records')

            headers = {'Content-Type': 'application/json'}
            response = requests.post(API_ENDPOINT, headers=headers, data=data)
            
            if response.status_code == 200:
                logging.info(f"Data sent to Power BI: {data}")
            else:
                logging.error(f"Failed to send data to Power BI: {response.status_code}, {response.text}")