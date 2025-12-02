from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sample message (customize these fields as you want)
message = {
    "taxonomy_id": 1001,
    "location": {"latitude": -17.82, "longitude": 31.05},
    "body_size": "medium",
    "migration_status": "resident"
}

producer.send('bird_sightings', value=message)
producer.flush()
print("Test message sent!")