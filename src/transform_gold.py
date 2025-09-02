# src/transform_gold.py
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
import os
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds


# === Paths ============================================================
DATA_ROOT = Path(os.getenv("AIRFLOW_DATA_ROOT", "/opt/airflow/data"))
SILVER_DIR = DATA_ROOT / "silver"
GOLD_DIR = DATA_ROOT / "gold"

# Schema Arrow explícito para evitar unificação string<->null
ARROW_SILVER_SCHEMA = pa.schema(
    [
        pa.field("country", pa.string()),
        pa.field("state", pa.string()),
        pa.field("brewery_type", pa.string()),
        pa.field("city", pa.string()),
    ]
)


# === Helpers ==========================================================
def _partition_dir_for(ingestion_date: str) -> Path:
    """
    silver/ingestion_date=YYYY-MM-DD
    (Se não existir, cairemos no diretório raiz do silver.)
    """
    return SILVER_DIR / f"ingestion_date={ingestion_date}"


def _silver_scan_root(run_date: str) -> Path:
    """
    Se houver partição por ingestion_date para o run_date, lê só ela.
    Caso contrário, lê o diretório silver inteiro.
    """
    part_dir = _partition_dir_for(run_date)
    return part_dir if part_dir.exists() else SILVER_DIR


def _load_all_silver_as_df(run_date: str) -> pd.DataFrame:
    """
    Lê os Parquets do silver (da partição do run_date se existir),
    forçando schema Arrow para evitar cast string->null.
    Também habilita partições Hive para materializar colunas
    vindas de diretórios (country=..., state=..., etc) caso existam.
    """
    scan_root = _silver_scan_root(run_date)

    dataset = ds.dataset(
        str(scan_root),
        format="parquet",
        partitioning="hive",     # autodetecta colunas de partição do caminho
        schema=ARROW_SILVER_SCHEMA,  # força colunas como string
    )

    table = dataset.to_table()
    df = table.to_pandas()

    # Garante presença e dtype das colunas esperadas
    expected_cols = ["country", "state", "brewery_type", "city"]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = pd.Series(dtype="string")
        else:
            df[col] = df[col].astype("string")

    return df[expected_cols]


def aggregate_breweries_by_type_location(df: pd.DataFrame) -> pd.DataFrame:
    keys = [c for c in ["country", "state", "brewery_type", "city"] if c in df.columns]
    out = df.groupby(keys, dropna=False).size().reset_index(name="qty")
    return out.sort_values(keys).reset_index(drop=True)


def _write_gold(df: pd.DataFrame, run_date: str) -> Path:
    """
    Salva o Gold particionado por run_date (mesmo padrão que você já usa).
    """
    ts = datetime.now(timezone.utc).strftime("%H%M%S")
    outdir = GOLD_DIR / f"run_date={run_date}"
    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / f"breweries_by_type_and_location_{ts}.parquet"
    df.to_parquet(path, engine="pyarrow", index=False)
    return path


# === Entry point chamado pela DAG ====================================
def run_gold(**context):
    run_date: str = context["ds"]  # YYYY-MM-DD
    df = _load_all_silver_as_df(run_date)
    gold = aggregate_breweries_by_type_location(df)
    p = _write_gold(gold, run_date)
    print(f"Wrote gold: {p}")
