import json
import pandas as pd
from pycountry_convert import country_name_to_country_alpha2, country_alpha2_to_continent_code

with open('/Users/wassim/Documents/Datalake challenge/datalake/data/raw/google trends/trends_par_pays.json', 'r', encoding='utf-8') as f:
    data = json.load(f)


def is_european_country(country_name):
    try:
        code = country_name_to_country_alpha2(country_name)
        continent = country_alpha2_to_continent_code(code)
        return continent == 'EU'
    except:
        return False

# === Filtrage des pays européens ===
europe_data = [entry for entry in data if is_european_country(entry['geoName'])]

# === Export ou affichage final ===
df = pd.DataFrame(europe_data)
print(df)

# Optionnel : export en CSV
df.to_csv('europe_only.csv', index=False, encoding='utf-8-sig')