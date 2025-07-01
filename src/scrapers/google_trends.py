from pytrends.request import TrendReq
import pandas as pd
import json
import time
import random

def get_interest_by_country(keywords):
    # Initialisation avec retry intégré
    pytrends = TrendReq(hl='en-US', tz=360, retries=3, backoff_factor=0.1)

    # Construction de la requête pour tous les mots-clés
    pytrends.build_payload(keywords, timeframe='today 12-m', geo='')

    # Récupération : intérêt par pays
    df = pytrends.interest_by_region(resolution='COUNTRY', inc_low_vol=True, inc_geo_code=False)

    # On garde seulement les colonnes utiles
    df = df[keywords]

    # On remet le nom des pays comme colonne
    df = df.reset_index()

    return df

def export_json(df, filename):
    # Export DataFrame vers JSON
    json_data = df.to_json(orient='records', force_ascii=False)
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(json_data)
    print(f"Export JSON terminé : {filename}")

if __name__ == "__main__":
    # Liste de mots-clés
    keywords = ['Python', 'React']

    print(f"Lancement de la collecte de données Google Trends pour : {keywords} ...")

    try:
        data = get_interest_by_country(keywords)
        export_json(data, 'trends_par_pays.json')
    except Exception as e:
        print(f"Erreur pendant l'extraction : {e}")