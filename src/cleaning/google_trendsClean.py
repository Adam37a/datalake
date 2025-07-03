import json
import pandas as pd
from pathlib import Path
from pycountry_convert import country_name_to_country_alpha2, country_alpha2_to_continent_code

# === Chemin vers le fichier JSON brut ===
BASE_DIR = Path(__file__).resolve().parents[2]
JSON_PATH = BASE_DIR / "data" / "raw" / "google trends" / "trends_par_pays.json"

def is_european_country(country_name):
    """
    Vérifie si un pays est en Europe à partir de son nom
    """
    try:
        code = country_name_to_country_alpha2(country_name)
        continent = country_alpha2_to_continent_code(code)
        return continent == 'EU'
    except:
        return False

def get_cleaned_google_trends_df():
    """
    Charge et nettoie les données Google Trends en gardant seulement les pays européens
    """
    # Charger les données JSON
    with open(JSON_PATH, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Filtrer les pays européens
    europe_data = [entry for entry in data if is_european_country(entry.get('geoName', ''))]

    # Convertir en DataFrame
    df = pd.DataFrame(europe_data)
    return df
