from kafka import KafkaConsumer
import json

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'mouse_events_topic',  # Topic to consume from
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
)

print("Listening for messages on 'mouse_events_topic'...")

# Consume messages
for message in consumer:
    event_data = message.value
    print(f'{message.value}')
