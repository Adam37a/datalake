import pandas as pd
from pathlib import Path
from deep_translator import GoogleTranslator

# === Chemin vers le fichier CSV brut ===
BASE_DIR = Path(__file__).resolve().parents[2]
CSV_PATH = BASE_DIR / "data" / "raw" / "github_trend" / "github_trending.csv"

def translate_text(text):
    """
    Traduit un texte en anglais s'il n'est pas déjà en anglais
    """
    if not isinstance(text, str) or text.strip() == "":
        return text  # Ignore les valeurs vides ou non textuelles
    try:
        # GoogleTranslator détecte la langue automatiquement
        return GoogleTranslator(source='auto', target='en').translate(text)
    except Exception as e:
        print(f"⚠️ Erreur traduction : {e}")
        return text  # Retourne le texte original en cas d'erreur

def get_cleaned_github_trends_df():
    """
    Charge et nettoie le fichier GitHub Trending :
    - Traduction des champs textuels en anglais
    """
    print(f"📄 Lecture de : {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, encoding="utf-8")

    # Colonnes à traduire (par exemple : description)
    text_columns = ["description"]
    for col in text_columns:
        if col in df.columns:
            print(f"🌐 Traduction de la colonne : {col}")
            df[col] = df[col].apply(translate_text)

    return df
