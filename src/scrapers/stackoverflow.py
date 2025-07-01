import os
import requests
import zipfile
from io import BytesIO

# Dossier cible pour les fichiers extraits
raw_dir = "raw"
os.makedirs(raw_dir, exist_ok=True)

# Années à traiter
years = range(2011, 2025)

base_url = "https://survey.stackoverflow.co/datasets/stack-overflow-developer-survey-{}.zip"

for year in years:
    url = base_url.format(year)
    print(f"Téléchargement pour l'année {year} depuis : {url}")

    try:
        response = requests.get(url)
        response.raise_for_status()

        with zipfile.ZipFile(BytesIO(response.content)) as z:
            # Trouver les fichiers CSV dans l'archive
            csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
            if not csv_files:
                print(f"❌ Aucun fichier CSV trouvé pour {year}")
                continue

            for i, csv_file in enumerate(csv_files):
                # Lecture du contenu du fichier
                with z.open(csv_file) as extracted_file:
                    content = extracted_file.read()

                # Crée un nom basé sur l'année
                if len(csv_files) == 1:
                    target_filename = f"{year} Stack Overflow Survey Results.csv"
                else:
                    target_filename = f"{year} - {i+1} Stack Overflow Survey Results.csv"

                target_path = os.path.join(raw_dir, target_filename)

                # Écriture dans le dossier .raw
                with open(target_path, "wb") as out_file:
                    out_file.write(content)

                print(f"✅ Fichier extrait : {target_filename}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Échec du téléchargement pour {year} : {e}")
    except zipfile.BadZipFile:
        print(f"❌ Archive invalide ou corrompue pour {year}")

print("\n✅ Traitement terminé.")
