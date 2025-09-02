# src/config.py
from pathlib import Path
import os
from datetime import date

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"

# se AIRFLOW setar a data, usamos; se não, usamos hoje
INGESTION_DATE = os.getenv("INGESTION_DATE", date.today().isoformat())
RUN_DATE = os.getenv("RUN_DATE", INGESTION_DATE)

BRONZE_DIR = DATA_DIR / "bronze" / f"ingestion_date={INGESTION_DATE}"
SILVER_DIR = DATA_DIR / "silver" / f"ingestion_date={INGESTION_DATE}"
GOLD_DIR = DATA_DIR / "gold" / f"run_date={RUN_DATE}"
