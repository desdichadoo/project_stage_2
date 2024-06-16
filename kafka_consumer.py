import yaml
import json
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from statistics import mean
import time

def load_config():
    with open("config/config.yaml", 'r') as ymlfile:
        return yaml.safe_load(ymlfile)

def create_kafka_consumer(config):
    while True:
        try:
            consumer = KafkaConsumer(
                config['kafka']['valid_data_topic'],
                bootstrap_servers=config['kafka']['bootstrap_servers'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            return consumer
        except NoBrokersAvailable:
            print("No Kafka brokers available. Retrying in 5 seconds...")
            time.sleep(5)

def create_kafka_producer(config):
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=config['kafka']['bootstrap_servers'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            return producer
        except NoBrokersAvailable:
            print("No Kafka brokers available. Retrying in 5 seconds...")
            time.sleep(5)

def clean_data(sensors):
    readings = sorted(sensors)
    differences = [abs(readings[i] - readings[i + 1]) for i in range(len(readings) - 1)]

    if all(diff < 2.0 for diff in differences):
        return mean(readings), "Reliable"
    elif len([d for d in differences if d >= 2.0]) == 1:
        return mean([readings[0], readings[1], readings[2]]), "UnreliableSensorReading"
    else:
        return None, "UnreliableRow"

def process_messages(consumer, producer_clean, producer_monitoring, config):
    for message in consumer:
        data = message.value
        sensors = [data['sensor1'], data['sensor2'], data['sensor3'], data['sensor4']]
        clean_value, status = clean_data(sensors)

        if clean_value is not None:
            producer_clean.send(config['kafka']['clean_data_topic'], value={
                "station_id": data['station_id'],
                "timestamp": data['timestamp'],
                "temperature": clean_value
            })
            if status == "Reliable":
                producer_monitoring.send(config['kafka']['monitoring_topic'], value={
                    "reason": status,
                    "data": data
                })
        else:
            producer_monitoring.send(config['kafka']['monitoring_topic'], value={
                "reason": status,
                "data": data
            })

if __name__ == "__main__":
    config = load_config()
    consumer = create_kafka_consumer(config)
    producer_clean = create_kafka_producer(config)
    producer_monitoring = create_kafka_producer(config)
    
    process_messages(consumer, producer_clean, producer_monitoring, config)
