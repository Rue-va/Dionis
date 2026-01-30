import pandas as pd
from pymongo import MongoClient
from fuzzywuzzy import process, fuzz
import sys
import os

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "bird_db"

def get_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    cls = list(db.audio_classifications.find())
    species = list(db.bird_species.find())
    return cls, species

def fuzzy_filter(species_names, search_term, cutoff=70):
    matches = process.extractBests(
        search_term, 
        species_names, 
        scorer=fuzz.token_sort_ratio, 
        score_cutoff=cutoff
    )
    return [m[0] for m in matches]

def prepare_report(fuzzy_search=None):
    cls, species = get_data()
    cls_df = pd.DataFrame(cls)
    
    if cls_df.empty:
        print("No classifications to report!")
        return

    # FIX 1: Safely extract species using isinstance check
    # This prevents the 'float' object has no attribute 'get' error
    cls_df['species'] = cls_df['classification'].apply(
        lambda x: x.get('species', None) if isinstance(x, dict) else None
    )
    
    # Filter out any rows with no species classification
    cls_df = cls_df[cls_df['species'].notna()]
    
    if fuzzy_search:
        all_species = cls_df['species'].unique().tolist()
        allowed = fuzzy_filter(all_species, fuzzy_search)
        print(f"Fuzzy search '{fuzzy_search}' matched: {allowed}")
        cls_df = cls_df[cls_df['species'].isin(allowed)]

    # FIX 2: Safely calculate average confidence by checking types inside the aggregation
    def calc_avg_conf(series):
        confidences = [v.get('confidence', 0) for v in series if isinstance(v, dict)]
        return pd.Series(confidences).mean() if confidences else 0

    report = cls_df.groupby('species').agg(
        num_classified=('species', 'count'),
        avg_confidence=('classification', calc_avg_conf)
    ).reset_index()

    # Merge with species for canonical/scientific name
    species_df = pd.DataFrame(species)
    if not species_df.empty:
        # Standardizing column names for the merge
        report = report.merge(
            species_df[['canonicalName', 'scientificName']],
            left_on='species', right_on='canonicalName', how='left'
        )

    report = report.sort_values("num_classified", ascending=False)
    
    outname = "bird_report.csv" if not fuzzy_search else f"bird_report_{fuzzy_search}.csv"
    report.to_csv(outname, index=False)
    print(f"Saved report to: {outname}")

    # Bonus: Bar chart visualization
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        if report.empty:
            print("Report is empty, skipping visualization.")
            return

        plt.figure(figsize=(10,5))
        sns.barplot(data=report, x='species', y='num_classified')
        plt.xticks(rotation=45)
        plt.title('Number of Classified Sightings per Species')
        plt.tight_layout()
        plt.savefig('bird_report_plot.png')
        print("Saved visualization to bird_report_plot.png")
    except Exception as e:
        print("Visualization skipped:", e)

if __name__ == "__main__":
    fuzzy_search = sys.argv[1] if len(sys.argv) > 1 else None
    prepare_report(fuzzy_search)