import requests
from pymongo import MongoClient

# Database Configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bird_db"
# The official mock website URL from your project requirements
SPECIES_URL = "https://aves.regoch.net/aves.json"

def species_exists(db):
    """Checks if the collection already has data to avoid unnecessary scraping."""
    return db.bird_species.count_documents({}) > 0

def scrape_and_store():
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    # 1. Requirement: If data exists, skip this step
    if species_exists(db):
        print("Species data already present in MongoDB. Skipping scraping.")
        return

    print(f"Fetching data from {SPECIES_URL}...")
    try:
        response = requests.get(SPECIES_URL)
        response.raise_for_status()
        data = response.json()  # This is a list of bird species
    except Exception as e:
        print(f"Error fetching data: {e}")
        return

    inserted_count = 0
    
    # 2. Requirement: Create a backbone by storing scraped data
    for bird in data:
        # Extract the 'key' from the API and convert to 'taxonomy_id'
        # This matches the ID used in your Kafka producer messages
        raw_key = bird.get("key")
        
        if raw_key is not None:
            try:
                # Standardize as an integer so lookup is reliable
                taxonomy_id = int(raw_key)
                bird["taxonomy_id"] = taxonomy_id
                
                # 3. Requirement: Safeguard for duplicates
                # Check if this specific bird is already in our 'backbone'
                if not db.bird_species.find_one({"taxonomy_id": taxonomy_id}):
                    db.bird_species.insert_one(bird)
                    inserted_count += 1
            except ValueError:
                print(f"Skipping bird with non-numeric key: {raw_key}")
                continue

    print(f"Successfully inserted {inserted_count} unique species into the backbone.")

if __name__ == "__main__":
    scrape_and_store()