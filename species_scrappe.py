import requests
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bird_db"
SPECIES_URL = "https://aves.regoch.net/aves.json"

def species_exists(db):
    return db.bird_species.count_documents({}) > 0

def scrape_and_store():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    if species_exists(db):
        print("Species data already present. Skipping scraping.")
        return

    response = requests.get(SPECIES_URL)
    data = response.json()  # This should be a list!

    inserted_count = 0
    for bird in data:
        # Safeguard for duplicates
        unique_id = bird.get("key") or bird.get("scientificName")
        if not db.bird_species.find_one({"key": unique_id}):
            db.bird_species.insert_one(bird)
            inserted_count += 1

    print(f"Inserted {inserted_count} unique species.")

if __name__ == "__main__":
    scrape_and_store()