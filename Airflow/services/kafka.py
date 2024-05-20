from kafka import KafkaProducer, KafkaConsumer
from json import dumps, loads
import logging
import pandas as pd
import requests


def kafka_producer(data):
    producer = KafkaProducer(
        value_serializer = lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers = ['localhost:9092']
    )
    message = data.to_dict()
    producer.send("kafka-playstore-apps", value=message)
    logging.info("Message sent")



# Configuración del consumidor de Kafka
def kafka_consumer():
    consumer = KafkaConsumer(
        'kafka-playstore-apps',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-1',
        value_deserializer=lambda m: loads(m.decode('utf-8')),
        bootstrap_servers=['localhost:9092']
    )

    for message in consumer:
        print(f"Received message: {message.value}")
        # Añadir lógica adicional para procesar los mensajes



"""
def kafka_consumer():
    API_ENDPOINT = 'https://api.powerbi.com/beta/YOUR_POWERBI_WORKSPACE_ID/datasets/YOUR_DATASET_ID/rows?key=YOUR_API_KEY'
    
    consumer = KafkaConsumer(
        'kafka-playstore-apps',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-1',
        value_deserializer=lambda m: loads(m.decode('utf-8')),
        bootstrap_servers=['localhost:9092']
    )

    for message in consumer:
        # Convert message to DataFrame
        df = pd.json_normalize(message.value)
        # Add any necessary processing here
        # Convert DataFrame to JSON
        data = df.to_json(orient='records')
        
        # Send data to Power BI
        headers = {'Content-Type': 'application/json'}
        response = requests.post(API_ENDPOINT, headers=headers, data=data)
        
        if response.status_code == 200:
            logging.info(f"Data sent to Power BI: {data}")
        else:
            logging.error(f"Failed to send data to Power BI: {response.status_code}, {response.text}")
"""
