import requests
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm  # barre de progression

# Définir les chemins
BASE_DIR = Path(__file__).resolve().parents[2]
raw_dir = BASE_DIR / "data" / "raw" / "hacker_news"
raw_dir.mkdir(parents=True, exist_ok=True)
output_file = raw_dir / "all_stories.json"

# Obtenir le dernier ID connu (maxitem)
max_id = requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json").json()

# Préparer les 10 000 derniers IDs
target_ids = list(range(max_id, max_id - 10000, -1))

# Fonction de récupération individuelle
def fetch_story(story_id):
    url = f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
    try:
        r = requests.get(url, timeout=5)
        data = r.json()
        if data and data.get("type") == "story":
            return data
    except:
        return None

# Téléchargement parallèle avec ThreadPool
stories = []
with ThreadPoolExecutor(max_workers=40) as executor:
    futures = {executor.submit(fetch_story, sid): sid for sid in target_ids}
    for future in tqdm(as_completed(futures), total=len(futures), desc="Fetching stories"):
        story = future.result()
        if story:
            stories.append(story)

# Sauvegarde
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(stories, f, indent=2, ensure_ascii=False)

print(f"{len(stories)} stories valides sauvegardées dans : {output_file}")
