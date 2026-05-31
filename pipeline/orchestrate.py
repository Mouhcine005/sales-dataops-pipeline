from dagster import job, op, In, Nothing
import subprocess
import sys
import os

VENV_PYTHON = sys.executable

def run_script(cmd):
    result = subprocess.run(cmd, shell=True)
    return result.returncode

@op
def ingest():
    """Étape 1 : Ingestion des données CSV."""
    print(f"Exécution de l'ingestion avec : {VENV_PYTHON}")
    exit_code = run_script(f'"{VENV_PYTHON}" pipeline/ingest.py')
    if exit_code != 0:
        raise Exception("L'ingestion a échoué")

@op(ins={"ingest_status": In(Nothing)})
def validate(ingest_status):
    """Étape 2 : Validation du schéma."""
    exit_code = run_script(f'"{VENV_PYTHON}" pipeline/validate.py')
    if exit_code != 0:
        raise Exception("La validation a échoué")

@op(ins={"validate_status": In(Nothing)})
def transform(validate_status):
    """Étape 3 : Exécution des modèles dbt."""
    dbt_path = os.path.join(os.path.dirname(VENV_PYTHON), "dbt")
    exit_code = run_script(f'cd dbt_pipeline && "{dbt_path}" run --profiles-dir .')
    if exit_code != 0:
        raise Exception("La transformation dbt a échoué")

@op(ins={"transform_status": In(Nothing)})
def test_data(transform_status):
    """Étape 4 : Exécution des tests dbt."""
    dbt_path = os.path.join(os.path.dirname(VENV_PYTHON), "dbt")
    exit_code = run_script(f'cd dbt_pipeline && "{dbt_path}" test --profiles-dir .')
    if exit_code != 0:
        raise Exception("Les tests dbt ont échoué")

@job
def ventes_pipeline():
    test_data(transform(validate(ingest())))