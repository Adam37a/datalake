import requests
from bs4 import BeautifulSoup
import csv
import time
from pathlib import Path
from datetime import datetime

# === Dossier de sortie ===
BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data" / "raw" / "github_trend"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def get_trending_url(period="daily"):
    base_url = "https://github.com/trending"
    if period in ["daily", "weekly", "monthly"]:
        return f"{base_url}?since={period}"
    else:
        return base_url

def scrape_github_trending(period="daily"):
    URL = get_trending_url(period)
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"
    }
    response = requests.get(URL, headers=headers)

    soup = BeautifulSoup(response.text, "html.parser")
    repo_list = soup.find_all("article", class_="Box-row")

    data = []

    for repo in repo_list:
        # Auteur / owner et nom repo
        full_name = repo.h2.text.strip().replace("\n", "").replace(" ", "")
        # full_name = owner/repo
        if "/" in full_name:
            author, name = full_name.split("/")
        else:
            author, name = full_name, ""

        # Description (peut être None)
        description_tag = repo.find("p", class_="col-9 color-fg-muted my-1 pr-4")
        description = description_tag.text.strip() if description_tag else ""

        # Langage (peut être None)
        lang_tag = repo.find("span", itemprop="programmingLanguage")
        language = lang_tag.text.strip() if lang_tag else ""

        # Nombre d'étoiles total
        stars_tag = repo.find("a", href=lambda x: x and x.endswith("/stargazers"))
        stars = stars_tag.text.strip().replace(",", "") if stars_tag else "0"

        # Étoiles sur la période (ex: today)
        stars_period_tag = repo.find("span", class_="d-inline-block float-sm-right")
        stars_period = stars_period_tag.text.strip().split(" ")[0] if stars_period_tag else "0"

        # URL repo
        repo_url = "https://github.com" + repo.h2.a["href"]

        data.append({
            "author": author,
            "name": name,
            "description": description,
            "language": language,
            "stars": int(stars.replace(",", "")),
            "stars_period": stars_period,
            "url": repo_url
        })

    return data

def save_to_csv(data, filename="github_trending.csv"):
    if not data:
        print("Aucune donnée à sauvegarder.")
        return

    filepath = DATA_DIR / filename
    keys = data[0].keys()
    with open(filepath, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

    print(f"✅ Scraping terminé. Données sauvegardées dans : {filepath}")

if __name__ == "__main__":
    trending_data = scrape_github_trending()
    save_to_csv(trending_data)
