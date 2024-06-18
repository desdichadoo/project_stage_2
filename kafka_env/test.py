from kafka import KafkaConsumer, KafkaProducer
import logging
import json
import time


logging.basicConfig(level=logging.INFO)

bootstrap_servers = 'localhost:9092'
valid_data_topic = 'valid_data'
clean_data_topic = 'clean_data'
monitoring_topic = 'monitoring'

sample_data = [{
                'ts': '2023-06-16T00:00:00Z',
                'station_id': 'station1',
                'sensor0': 20.5,
                'sensor1': 20.7,
                'sensor2': 20.6,
                'sensor3': 20.8
            }]

kafka_producer = KafkaProducer(bootstrap_servers = bootstrap_servers,
                               value_serializer=lambda x: json.dumps(x).encode('utf-8'))

for data in sample_data:
    kafka_producer.send(clean_data_topic, value=data)
    print(f'Sent: {data}')

kafka_producer.flush()
kafka_producer.close()

time.sleep(5)


kafka_consumer = KafkaConsumer(clean_data_topic,
                               bootstrap_servers = bootstrap_servers,
                               value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                               auto_offset_reset='earliest',
                                enable_auto_commit=False)

print("Starting consumer...")
for message in kafka_consumer:
    received_data = message.value
    print(f"Received: {received_data}")
    if received_data in sample_data:
        sample_data.remove(received_data)
    if not sample_data:
        break