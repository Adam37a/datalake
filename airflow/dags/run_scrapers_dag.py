from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pathlib import Path

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

BASE_DIR = Path(__file__).resolve().parents[2]
SCRAPER_DIR = BASE_DIR / "src" / "scrapers"

with DAG(
    dag_id='run_scrapers_dag',
    default_args=default_args,
    description='Exécute tous les scrapers Python dans src/scrapers',
    schedule_interval='@daily',  # Exécute tous les jours à minuit
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=['scrapers']
) as dag:

    for script in SCRAPER_DIR.glob("*.py"):
        task = BashOperator(
            task_id=f"run_{script.stem}",
            bash_command=f"python {script}",
            dag=dag
        )