from kafka import KafkaConsumer
import json
from pymongo import MongoClient

KAFKA_TOPIC = "bird_sightings"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bird_db"

def store_sightings():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=['localhost:9092'],
        api_version=(0, 10, 1),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=5000 # <-- timeout after 5s of no new messages so that when i do run snakemake it does not appear stuck waiting for new messages in an infinite loop.
    )
    for message in consumer:
        sighting = message.value
        if 'taxonomy_id' in sighting and 'location' in sighting:
            db.sightings.insert_one(sighting)
            print('Stored:', sighting)
        else:
            print('Skipped invalid:', sighting)
    print("No more messages. Exiting.")

if __name__ == '__main__':
    store_sightings()