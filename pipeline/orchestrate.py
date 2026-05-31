from dagster import job, op, In, Nothing
import subprocess
import sys
import os
from pathlib import Path

VENV_PYTHON = sys.executable
BASE_DIR = Path(__file__).parent.parent
DB_PATH = str(BASE_DIR / "ventes.duckdb")

def run_script(cmd, env=None):
    result = subprocess.run(cmd, shell=True, env=env)
    return result.returncode

def get_dbt_env():
    """Environment with DuckDB path for dbt."""
    env = os.environ.copy()
    env["DBT_DUCKDB_PATH"] = DB_PATH
    return env

@op
def ingest():
    exit_code = run_script(f'"{VENV_PYTHON}" pipeline/ingest.py')
    if exit_code != 0:
        raise Exception("L'ingestion a échoué")

@op(ins={"ingest_status": In(Nothing)})
def validate():
    exit_code = run_script(f'"{VENV_PYTHON}" pipeline/validate.py')
    if exit_code != 0:
        raise Exception("La validation a échoué")

@op(ins={"validate_status": In(Nothing)})
def transform():
    dbt_path = os.path.join(os.path.dirname(VENV_PYTHON), "dbt")
    exit_code = run_script(
        f'cd dbt_pipeline && "{dbt_path}" run --profiles-dir .',
        env=get_dbt_env()
    )
    if exit_code != 0:
        raise Exception("La transformation dbt a échoué")

@op(ins={"transform_status": In(Nothing)})
def test_data():
    dbt_path = os.path.join(os.path.dirname(VENV_PYTHON), "dbt")
    exit_code = run_script(
        f'cd dbt_pipeline && "{dbt_path}" test --profiles-dir .',
        env=get_dbt_env()
    )
    if exit_code != 0:
        raise Exception("Les tests dbt ont échoué")

@job
def ventes_pipeline():
    test_data(transform(validate(ingest())))