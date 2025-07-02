import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging

# Config de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ⚙️ Paramètres de connexion PostgreSQL
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "admin"
DB_HOST = "localhost"
DB_PORT = "5432"


BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw")
)

EXCLUDED_SOURCES = {"stackoverflow"}

# Connexion SQLAlchemy
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def normalize_table_name(name: str) -> str:
    return name.lower().replace("-", "_").replace(" ", "_")

def load_data_to_postgres():
    for source in os.listdir(BASE_DIR):
        if source.lower() in EXCLUDED_SOURCES:
            logger.info(f"⏩ Source ignorée : {source}")
            continue

        source_path = os.path.join(BASE_DIR, source)
        if not os.path.isdir(source_path):
            continue

        table_name = normalize_table_name(source)
        logger.info(f"📦 Traitement de la source : {source} → Table : {table_name}")

        for file in os.listdir(source_path):
            file_path = os.path.join(source_path, file)
            if file.endswith(".csv"):
                df = pd.read_csv(file_path, encoding="ISO-8859-1")
            elif file.endswith(".json"):
                df = pd.read_json(file_path, lines=False)
            else:
                logger.warning(f"⚠️ Fichier ignoré (format non supporté) : {file}")
                continue

            logger.info(f"📄 Chargement de {file} ({len(df)} lignes)")

            try:
                df.to_sql(table_name, con=engine, if_exists='append', index=False)
                logger.info(f"✅ Données insérées dans la table : {table_name}")
            except Exception as e:
                logger.error(f"❌ Erreur d'insertion pour {file} : {e}")

if __name__ == "__main__":
    load_data_to_postgres()