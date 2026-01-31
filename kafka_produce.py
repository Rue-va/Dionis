import random
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# A list of different biological observations to simulate different sources
varied_observations = [
    {"body_size": "medium", "migration_status": "resident", "flight_pattern": "undulating"},
    {"body_temperature": "41C", "habitat": "wetlands", "plumage": "bright"},
    {"diet": "insectivore", "nesting": "cavity", "clutch_size": 4},
    {"wingspan": "30cm", "activity_period": "diurnal"}
]

# Randomly pick one to show your DB can handle "varying biological data"
message = {
    "taxonomy_id": 2473325,
    "location": {"latitude": -17.82, "longitude": 31.05},
    **random.choice(varied_observations) # This adds the varying fields
}

producer.send('bird_sightings', value=message)
producer.flush()
print(f"Sent varied observation: {message}")