import requests
import json
import logging
from datetime import datetime
from pathlib import Path

# === Setup Logging ===
BASE_DIR = Path(__file__).resolve().parents[2]
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True, parents=True)

log_file = LOG_DIR / f"github_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))
logging.getLogger().addHandler(console)

# === GitHub API Config ===
CONFIG_PATH = BASE_DIR / "config" / "github_config.json"
with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

TOKEN = config.get("token")
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/vnd.github+json"
}

# === Data output directory ===
DATA_DIR = BASE_DIR / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# === GitHub API functions ===
def fetch_repositories(language="python", max_repos=30):
    url = "https://api.github.com/search/repositories"
    params = {
        "q": f"language:{language}",
        "sort": "stars",
        "order": "desc",
        "per_page": min(max_repos, 100)
    }

    try:
        response = requests.get(url, headers=HEADERS, params=params)
        response.raise_for_status()
        return response.json()["items"]
    except Exception as e:
        logging.error(f"Erreur récupération des repos : {e}")
        return []

def get_user_location(username):
    url = f"https://api.github.com/users/{username}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json().get("location", "Non renseignée")
    except Exception as e:
        logging.warning(f"❌ Erreur localisation pour {username} : {e}")
        return "Non disponible"

def save_results(results):
    filename = DATA_DIR / f"github_repos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    logging.info(f"📁 Fichier sauvegardé : {filename}")

# === Pipeline ===
def run_scraper():
    logging.info("🚀 Lancement du scraper GitHub")
    repos = fetch_repositories(language="python", max_repos=30)
    logging.info(f"🔍 {len(repos)} dépôts trouvés")

    results = []
    for repo in repos:
        owner = repo["owner"]["login"]
        location = get_user_location(owner)
        results.append({
            "name": repo["name"],
            "owner": owner,
            "stars": repo["stargazers_count"],
            "language": repo["language"],
            "owner_location": location
        })

    save_results(results)
    logging.info("✅ Fin du scraping GitHub")

if __name__ == "__main__":
    run_scraper()
