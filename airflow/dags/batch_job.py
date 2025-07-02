from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess

SCRAPER_DIR = os.path.expanduser("/Users/martinprevot/Documents/scrapping/datalake/src/scrapers")

default_args = {
    'owner': 'martin',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def run_scraper(script_path):
    def _run():
        print(f"Running: {script_path}")
        result = subprocess.run(["python3", script_path], capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print(f"Error in {os.path.basename(script_path)}:")
            print(result.stderr)
    return _run

with DAG(
    dag_id='run_each_scraper_separately_every_15min',
    default_args=default_args,
    start_date=datetime(2025, 7, 2),
    schedule_interval='*/15 * * * *',  # toutes les 15 minutes
    catchup=False,
    tags=['scraping', 'batch'],
) as dag:

    tasks = []

    for filename in os.listdir(SCRAPER_DIR):
        if filename.endswith(".py"):
            script_path = os.path.join(SCRAPER_DIR, filename)
            task = PythonOperator(
                task_id=f"run_{filename.replace('.py','')}",
                python_callable=run_scraper(script_path),
            )
            tasks.append(task)

