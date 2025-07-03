import json
import pandas as pd
from pathlib import Path

# === Chemin vers le dossier adzuna ===
BASE_DIR = Path(__file__).resolve().parents[2]
ADZUNA_DIR = BASE_DIR / "data" / "raw" / "adzuna"

def get_country_from_filename(filename):
    """
    Extrait le code pays (2 lettres) depuis le nom du fichier JSON
    Exemple : 'at_it_jobs_history_20250701_101318.json' → 'AT'
    """
    return filename.split("_")[0].upper()

def get_cleaned_adzuna_df():
    """
    Charge et nettoie toutes les données Adzuna JSON :
    - Pays
    - Date (YYYY-MM)
    - Salaire
    """
    records = []

    for json_file in ADZUNA_DIR.glob("*.json"):
        country = get_country_from_filename(json_file.name)

        # Charger le JSON
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Extraire les données mensuelles
        month_data = data.get("month", {})
        for date, salary in month_data.items():
            records.append({
                "country": country,
                "date": date,
                "salary": salary
            })

    # Convertir en DataFrame
    df = pd.DataFrame(records)
    print(f"📊 Adzuna DataFrame créé : {len(df)} lignes")
    return df
