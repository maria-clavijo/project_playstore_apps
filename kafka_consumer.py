from Airflow.services.kafka import kafka_consumer
from Airflow.loadb import create_merge_db

if __name__ == '__main__':
    create_merge_db()
    kafka_consumer()