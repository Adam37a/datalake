# cleaning_glassdoor.py
import pandas as pd
import re
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
CSV_PATH = BASE_DIR / "data" / "raw" / "glassdoor" / "glassdoor.csv"

TECH_SKILLS = [
    "Python", "R", "Java", "C++", "C#", "JavaScript", "TypeScript",
    "SQL", "NoSQL", "Hadoop", "Spark", "Kafka", "Docker", "Kubernetes",
    "AWS", "Azure", "GCP", "Tableau", "Power BI", "Excel", "TensorFlow",
    "PyTorch", "DevOps", "Git", "Linux", "Machine Learning", "Deep Learning",
    "AfterData", "Scikit-learn", "FastAPI", "Flask", "Airflow", "Anglais"
]

def extract_skills(description):
    if not isinstance(description, str):
        return ""
    found = set()
    for skill in TECH_SKILLS:
        pattern = r"\b" + re.escape(skill) + r"\b"
        if re.search(pattern, description, flags=re.IGNORECASE):
            found.add(skill)
    return ", ".join(sorted(found))

def get_cleaned_glassdoor_df():
    df = pd.read_csv(CSV_PATH, encoding="utf-8")
    desc_col = next((col for col in df.columns if col.lower() in ["description", "job_description", "details", "missions"]), None)
    if not desc_col:
        raise ValueError("❌ Aucune colonne de description trouvée.")
    df["compétences"] = df[desc_col].apply(extract_skills)
    return df
