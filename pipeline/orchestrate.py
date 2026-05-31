from dagster import job, op
import os

@op
def ingest():
    exit_code = os.system("python pipeline/ingest.py")
    if exit_code != 0: raise Exception("L'ingestion a échoué")

@op
def validate(context, ingest_result=None):
    exit_code = os.system("python pipeline/validate.py")
    if exit_code != 0: raise Exception("La validation a échoué")

@op
def transform(context, validate_result=None):
    exit_code = os.system("cd dbt_pipeline && dbt run --profiles-dir .")
    if exit_code != 0: raise Exception("La transformation dbt a échoué")

@op
def test_data(context, transform_result=None):
    exit_code = os.system("cd dbt_pipeline && dbt test --profiles-dir .")
    if exit_code != 0: raise Exception("Les tests dbt ont échoué")

@job
def ventes_pipeline():
    # Enchaînement séquentiel des dépendances
    res_ingest = ingest()
    res_validate = validate(res_ingest)
    res_transform = transform(res_validate)
    test_data(res_transform)