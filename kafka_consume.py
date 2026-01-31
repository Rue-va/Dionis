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
        consumer_timeout_ms=5000 
    )

    print("Listening for messages...")

    for message in consumer:
        sighting = message.value
        
        # 1. Basic validation for required fields
        if 'taxonomy_id' in sighting and 'location' in sighting:
            
            try:
                # FIX 1: Ensure taxonomy_id is an integer to match MongoDB's format
                search_id = int(sighting['taxonomy_id'])
                
                # FIX 2: Change 'db.species' to 'db.bird_species' to match your scraper
                # This satisfies LO3: Linking bird observations to species info
                species_record = db.bird_species.find_one({"taxonomy_id": search_id})
                
                if species_record:
                    # Store the sighting, including all varying biological fields
                    db.sightings.insert_one(sighting)
                    
                    # Try to get the name for the print statement
                    name = species_record.get('scientificName') or species_record.get('canonicalName')
                    print(f"Stored valid sighting for: {name} (ID: {search_id})")
                else:
                    print(f"Skipped: Taxonomy ID {search_id} not found in 'bird_species' collection.")
            
            except (ValueError, TypeError):
                print(f"Skipped: Invalid Taxonomy ID format in message: {sighting['taxonomy_id']}")
        else:
            print('Skipped: Message missing required taxonomy_id or location.')

    print("No more messages. Exiting.")

if __name__ == '__main__':
    store_sightings()