import pandas as pd
import duckdb
from pathlib import Path

# Always relative to this script's location, regardless of where it's called from
BASE_DIR = Path(__file__).parent.parent
csv_path = BASE_DIR / "data" / "ventes.csv"
db_path = BASE_DIR / "ventes.duckdb"

# Lecture et chargement dans DuckDB
df = pd.read_csv(csv_path)
con = duckdb.connect(str(db_path))
con.execute("CREATE OR REPLACE TABLE ventes_raw AS SELECT * FROM df")
con.close()

print(f"Ingestion terminée : table ventes_raw créée dans DuckDB")
print(f"CSV lu depuis : {csv_path}")
print(f"DuckDB : {db_path}")