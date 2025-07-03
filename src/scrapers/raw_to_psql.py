import os
import sys
import pandas as pd
from sqlalchemy import create_engine
import logging
from pathlib import Path

# Ajouter le dossier parent de 'cleaning' au sys.path
sys.path.append(str(Path(__file__).resolve().parents[1]))

# Imports des fonctions de nettoyage
from cleaning.cleaning_glassdoor import get_cleaned_glassdoor_df
from cleaning.google_trendsClean import get_cleaned_google_trends_df
from cleaning.github_clean import get_cleaned_github_trends_df
from cleaning.adzuna_clean import get_cleaned_adzuna_df

# === Config de logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# === Paramètres de connexion PostgreSQL ===
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "admin"
DB_HOST = "localhost"
DB_PORT = "5432"

# === Répertoire des données brutes ===
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw")
)

EXCLUDED_SOURCES = {"stack_overflow"}

# === Connexion SQLAlchemy ===
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def normalize_table_name(name: str) -> str:
    """
    Normalise le nom de la table (minuscules, remplace espaces et tirets)
    """
    return name.lower().replace("-", "_").replace(" ", "_")

def load_data_to_postgres():
    # ✅ Cas spécial : Glassdoor
    try:
        logger.info("🧽 Nettoyage des données Glassdoor...")
        df_glassdoor = get_cleaned_glassdoor_df()
        df_glassdoor.to_sql("glassdoor", con=engine, if_exists="append", index=False)
        logger.info("✅ Données Glassdoor insérées (avec enrichissement des compétences)")
    except Exception as e:
        logger.error(f"❌ Erreur nettoyage/insertion Glassdoor : {e}")

    # ✅ Cas spécial : Google Trends
    try:
        logger.info("🧽 Nettoyage des données Google Trends...")
        df_google_trends = get_cleaned_google_trends_df()
        df_google_trends.to_sql("google_trends", con=engine, if_exists="append", index=False)
        logger.info("✅ Données Google Trends insérées (pays européens uniquement)")
    except Exception as e:
        logger.error(f"❌ Erreur nettoyage/insertion Google Trends : {e}")

    # ✅ Cas spécial : GitHub Trends
    try:
        logger.info("🧽 Nettoyage des données GitHub Trends...")
        df_github_trends = get_cleaned_github_trends_df()
        df_github_trends.to_sql("github_trend", con=engine, if_exists="append", index=False)
        logger.info("✅ Données GitHub Trends insérées (avec traduction en anglais)")
    except Exception as e:
        logger.error(f"❌ Erreur nettoyage/insertion GitHub Trends : {e}")

    # ✅ Cas spécial : Adzuna
    try:
        logger.info("🧽 Nettoyage des données Adzuna...")
        df_adzuna = get_cleaned_adzuna_df()
        df_adzuna.to_sql("adzuna", con=engine, if_exists="append", index=False)
        logger.info("✅ Données Adzuna insérées (pays, dates et salaires)")
    except Exception as e:
        logger.error(f"❌ Erreur nettoyage/insertion Adzuna : {e}")

    # 🔁 Parcours des autres sources
    for source in os.listdir(BASE_DIR):
        normalized_source = source.lower()
        if normalized_source in EXCLUDED_SOURCES or normalized_source in {"glassdoor", "google trends", "github_trend", "adzuna"}:
            logger.info(f"⏩ Source ignorée : {source}")
            continue

        source_path = os.path.join(BASE_DIR, source)
        if not os.path.isdir(source_path):
            continue

        table_name = normalize_table_name(source)
        logger.info(f"📦 Traitement de la source : {source} → Table : {table_name}")

        for file in os.listdir(source_path):
            file_path = os.path.join(source_path, file)
            try:
                if file.endswith(".csv"):
                    df = pd.read_csv(file_path, encoding="utf-8")
                elif file.endswith(".json"):
                    df = pd.read_json(file_path, lines=False)
                else:
                    logger.warning(f"⚠️ Fichier ignoré (format non supporté) : {file}")
                    continue

                logger.info(f"📄 Chargement de {file} ({len(df)} lignes)")

                df.to_sql(table_name, con=engine, if_exists='append', index=False)
                logger.info(f"✅ Données insérées dans la table : {table_name}")
            except Exception as e:
                logger.error(f"❌ Erreur d'insertion pour {file} : {e}")

if __name__ == "__main__":
    load_data_to_postgres()
