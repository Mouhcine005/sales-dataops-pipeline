from dagster import job, op
import os
import sys

# Récupère automatiquement le chemin exact du Python de l'environnement virtuel actif
VENV_PYTHON = sys.executable

@op
def ingest():
    """Étape 1 : Ingestion des données CSV."""
    print(f"Exécution de l'ingestion avec : {VENV_PYTHON}")
    exit_code = os.system(f'"{VENV_PYTHON}" pipeline/ingest.py')
    if exit_code != 0: 
        raise Exception("L'ingestion a échoué")

@op
def validate(ingest_status):
    """Étape 2 : Validation du schéma."""
    exit_code = os.system(f'"{VENV_PYTHON}" pipeline/validate.py')
    if exit_code != 0: 
        raise Exception("La validation a échoué")

@op
def transform(validate_status):
    """Étape 3 : Exécution des modèles dbt."""
    # Utilise le dbt installé dans l'environnement virtuel pour Windows
    dbt_path = os.path.join(os.path.dirname(VENV_PYTHON), "dbt")
    exit_code = os.system(f'cd dbt_pipeline && "{dbt_path}" run --profiles-dir .')
    if exit_code != 0: 
        raise Exception("La transformation dbt a échoué")

@op
def test_data(transform_status):
    """Étape 4 : Exécution des tests dbt."""
    dbt_path = os.path.join(os.path.dirname(VENV_PYTHON), "dbt")
    exit_code = os.system(f'cd dbt_pipeline && "{dbt_path}" test --profiles-dir .')
    if exit_code != 0: 
        raise Exception("Les tests dbt ont échoué")

@job
def ventes_pipeline():
    """Définition du graphe de dépendances (Pipeline DAG)."""
    test_data(transform(validate(ingest())))