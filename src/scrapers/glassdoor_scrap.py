import logging
import csv
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
from pathlib import Path


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

URL = "https://www.glassdoor.fr/Emploi/france-data-emplois-SRCH_IL.0,6_IN86_KO7,11.htm"


from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
import time


from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

def close_login_popup(driver, timeout=5):
    try:
        # Attendre la présence d'un bouton 'CloseButton' visible
        wait = WebDriverWait(driver, timeout)
        close_button = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, 'CloseButton')))
        close_button.click()
        logger.info("Popup de connexion fermée.")
        time.sleep(1)  # pause pour que la fermeture soit prise en compte
    except TimeoutException:
        logger.info("Popup de connexion non présente.")
    except (NoSuchElementException, ElementClickInterceptedException) as e:
        logger.warning(f"Erreur en tentant de fermer la popup : {e}")


def scroll_and_click_load_more(driver, pause_time=2, max_attempts=1000, scroll_offset=200):
    last_count = 0
    attempts = 0

    while attempts < max_attempts:
        # Scroll un peu avant le bas de la page pour que le bouton soit visible
        scroll_height = driver.execute_script("return document.body.scrollHeight")
        scroll_position = scroll_height - scroll_offset
        driver.execute_script(f"window.scrollTo(0, {scroll_position});")
        time.sleep(pause_time)

        try:
            load_more_button = driver.find_element(By.XPATH, "//button[contains(., \"Voir plus d'offres d'emplois\")]")
            if load_more_button.is_displayed() and load_more_button.is_enabled():
                load_more_button.click()
                print("Bouton 'Voir plus d'offres d'emplois' cliqué")
                time.sleep(pause_time)

                # Fermeture de la popup login après le clic
                close_login_popup(driver)

            else:
                print("Bouton 'Voir plus' non cliquable")
                break
        except (NoSuchElementException, ElementNotInteractableException):
            print("Bouton 'Voir plus' non trouvé ou non interactif, fin du chargement")
            break

        jobs_cards = driver.find_elements(By.CSS_SELECTOR, "li.react-job-listing")
        current_count = len(jobs_cards)

        if current_count > last_count:
            last_count = current_count
            attempts = 0
        else:
            attempts += 1

    print(f"Chargement terminé, nombre total d'offres : {last_count}")


def export_to_csv(data, filename='jobs3.csv'):
    if not data:
        logging.warning("Aucune donnée à exporter.")
        return

    BASE_DIR = Path(__file__).resolve().parents[2]
    EXPORT_DIR = BASE_DIR / "data" / "raw" / "glassdoor"
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    filepath = EXPORT_DIR / filename

    keys = data[0].keys()
    with open(filepath, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)

    logging.info(f"✅ Export CSV effectué dans : {filepath}")


def get_driver():
    options = Options()
    # options.add_argument("--headless")  # Décommente pour mode sans fenêtre
    options.add_argument("--start-maximized")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def accept_cookies(driver):
    try:
        btn = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Accepter')]"))
        )
        btn.click()
        logger.info("Bandeau cookies accepté.")
    except:
        logger.info("Aucun bandeau cookies détecté.")

def extract_jobs(driver):
    try:
        # Attendre que des titres apparaissent
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'a[data-test="job-title"]'))
        )
        titles = driver.find_elements(By.CSS_SELECTOR, 'a[data-test="job-title"]')
        logger.info(f"{len(titles)} offres détectées.")

        jobs = []

        for title_elem in titles:
            try:
                card = title_elem.find_element(By.XPATH, "./ancestor::div[1]")
                title = title_elem.text

                try:
                    location = card.find_element(By.CSS_SELECTOR, 'div[data-test="emp-location"]').text
                except:
                    location = "Non renseigné"

                try:
                    company = card.find_element(By.CSS_SELECTOR, '.EmployerProfile_compactEmployerName__9MGcV').text
                except:
                    company = "Non renseignée"

                try:
                    salary = card.find_element(By.CSS_SELECTOR, 'div[data-test="detailSalary"]').text
                except:
                    salary = "Non renseigné"

                try:
                    rating = card.find_element(By.CSS_SELECTOR, '.rating-single-star_RatingText__XENmU').text
                except:
                    rating = "Non noté"

                try:
                    description = card.find_element(By.CSS_SELECTOR, 'div.JobCard_jobDescriptionSnippet__l1tnl').text
                except:
                    description = "Non renseignée"

                jobs.append({
                    "poste": title,
                    "lieu": location,
                    "entreprise": company,
                    "salaire": salary,
                    "note": rating,
                    "description": description
                })

            except Exception as e:
                logger.warning(f"Erreur lors du parsing d'une offre : {e}")

        return jobs

    except Exception as e:
        logger.error(f"Erreur d'extraction des titres : {e}")
        return []


def main():
    driver = get_driver()
    try:
        driver.get(URL)
        logger.info(f"Page chargée : {URL}")
        accept_cookies(driver)
        scroll_and_click_load_more(driver)
        jobs = extract_jobs(driver)

        for job in jobs:
            logger.info(job)

        logger.info(f"{len(jobs)} offres extraites.")
        export_to_csv(jobs)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
