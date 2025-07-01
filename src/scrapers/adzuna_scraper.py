import requests
import json
import logging
from datetime import datetime
from pathlib import Path

# === Initialisation logging ===
BASE_DIR = Path(__file__).resolve().parents[2]
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

log_file = LOG_DIR / f"adzuna_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# === Chargement de la configuration ===
CONFIG_PATH = BASE_DIR / "config" / "adzuna_config.json"
with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

APP_ID = config["app_id"]
APP_KEY = config["app_key"]
COUNTRIES = config["countries"]

# === Mapping code ISO vers noms complets pour location0 ===
COUNTRY_NAME_MAP = {
    "at": "Austria",
    "be": "Belgium",
    "ch": "Switzerland",
    "de": "Germany",
    "es": "Spain",
    "fr": "France",
    "gb": "UK",
    "it": "Italy",
    "nl": "Netherlands",
    "pl": "Poland"
}

# === Dossier pour stocker les fichiers ===
DATA_DIR = BASE_DIR / "data" / "raw" / "adzuna"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# === Fonction de scraping ===
def fetch_country_it_jobs(country_code):
    url = f"https://api.adzuna.com/v1/api/jobs/{country_code}/history"
    location_name = COUNTRY_NAME_MAP.get(country_code)

    if not location_name:
        logging.warning(f"❌ Pays non reconnu : {country_code}")
        return None

    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        # "location0": location_name,
        "category": "it-jobs"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        logging.info(f"✅ Données récupérées pour {country_code.upper()} ({location_name})")
        return response.json()
    except requests.RequestException as e:
        logging.error(f"❌ Erreur pour {country_code.upper()} : {e}")
        return None

# === Sauvegarde fichier JSON ===
def save_json(data, country_code):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = DATA_DIR / f"{country_code}_it_jobs_history_{timestamp}.json"
    try:
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)
        logging.info(f"📁 Fichier sauvegardé : {filename}")
    except Exception as e:
        logging.error(f"Erreur lors de la sauvegarde pour {country_code.upper()} : {e}")

# === Exécution principale ===
def run_scraper():
    logging.info("🚀 Lancement du scraper Adzuna multi-pays (/history)")
    for country_code in COUNTRIES:
        logging.info(f"🌍 Pays : {country_code.upper()}")
        data = fetch_country_it_jobs(country_code)
        if data:
            save_json(data, country_code)
    logging.info("✅ Fin du scraping")

if __name__ == "__main__":
    run_scraper()
