from dagster import job, op
import subprocess
import sys
import os

VENV_PYTHON = sys.executable

def run_script(cmd):
    """Run a command and return exit code."""
    result = subprocess.run(cmd, shell=True)
    return result.returncode

@op
def ingest():
    """Étape 1 : Ingestion des données CSV."""
    print(f"Exécution de l'ingestion avec : {VENV_PYTHON}")
    exit_code = run_script(f'"{VENV_PYTHON}" pipeline/ingest.py')
    if exit_code != 0:
        raise Exception("L'ingestion a échoué")

@op
def validate(context, ingest_status: None):
    """Étape 2 : Validation du schéma."""
    exit_code = run_script(f'"{VENV_PYTHON}" pipeline/validate.py')
    if exit_code != 0:
        raise Exception("La validation a échoué")

@op
def transform(context, validate_status: None):
    """Étape 3 : Exécution des modèles dbt."""
    dbt_path = os.path.join(os.path.dirname(VENV_PYTHON), "dbt")
    exit_code = run_script(f'cd dbt_pipeline && "{dbt_path}" run --profiles-dir .')
    if exit_code != 0:
        raise Exception("La transformation dbt a échoué")

@op
def test_data(context, transform_status: None):
    """Étape 4 : Exécution des tests dbt."""
    dbt_path = os.path.join(os.path.dirname(VENV_PYTHON), "dbt")
    exit_code = run_script(f'cd dbt_pipeline && "{dbt_path}" test --profiles-dir .')
    if exit_code != 0:
        raise Exception("Les tests dbt ont échoué")

@job
def ventes_pipeline():
    """Définition du graphe de dépendances (Pipeline DAG)."""
    test_data(transform(validate(ingest())))