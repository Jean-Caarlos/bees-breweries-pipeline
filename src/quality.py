# src/quality.py
from __future__ import annotations

from pathlib import Path
from datetime import datetime
import os
import pandas as pd

# evite conflito de alias com pandera
import pyarrow as _pa
import pyarrow.dataset as _ds
import pandera as pa
from pandera import Column, DataFrameSchema, Check


DATA_ROOT = Path(os.getenv("AIRFLOW_DATA_ROOT", "/opt/airflow/data"))
SILVER_DIR = DATA_ROOT / "silver"

# ------------------------------------------------------------
# Tipos permitidos (expandido p/ refletir a API atual)
# ------------------------------------------------------------
ALLOWED_BREWERY_TYPES = {
    "micro",
    "brewpub",
    "planning",
    "regional",
    "contract",
    "large",
    "nano",
    "bar",
    "proprietor",
    "closed",
    # adicionados
    "taproom",
    "beergarden",
}

# sinônimos/normalizações comuns
TYPE_SYNONYMS = {
    "brew_pub": "brewpub",
    "beer_garden": "beergarden",
    "beer-garden": "beergarden",
    "beer garden": "beergarden",
}

# Arrow schema apenas com strings para evitar cast string->null
ARROW_SILVER_SCHEMA = _pa.schema(
    [
        _pa.field("country", _pa.string()),
        _pa.field("state", _pa.string()),
        _pa.field("brewery_type", _pa.string()),
        _pa.field("city", _pa.string()),
    ]
)

# Pandera schema mínimo (após limpeza/normalização)
SilverSchema = DataFrameSchema(
    {
        "country": Column(pa.String, nullable=True),
        "state": Column(pa.String, nullable=True),
        "city": Column(pa.String, nullable=True),
        "brewery_type": Column(
            pa.String,
            checks=[Check.isin(sorted(ALLOWED_BREWERY_TYPES))],
            nullable=False,
        ),
    },
    coerce=True,
    strict=False,
)


def _partition_dir_for(ingestion_date: str) -> Path:
    # silver/ingestion_date=YYYY-MM-DD (padrão que você está usando)
    return SILVER_DIR / f"ingestion_date={ingestion_date}"


def _scan_root_for(run_date: str) -> Path:
    part = _partition_dir_for(run_date)
    return part if part.exists() else SILVER_DIR


def _load_silver_df(run_date: str) -> pd.DataFrame:
    root = _scan_root_for(run_date)
    if not root.exists():
        raise FileNotFoundError(f"Pasta do silver não encontrada: {root}")

    dataset = _ds.dataset(
        str(root),
        format="parquet",
        partitioning="hive",
        schema=ARROW_SILVER_SCHEMA,
    )
    table = dataset.to_table()
    df = table.to_pandas()

    # garante colunas necessárias como string dtype
    for col in ["country", "state", "city", "brewery_type"]:
        if col not in df.columns:
            df[col] = pd.Series(dtype="string")
        df[col] = df[col].astype("string")

    return df[["country", "state", "city", "brewery_type"]]


def _normalize_brewery_type(s: pd.Series) -> pd.Series:
    # lower/trim, trocar espaços/hífens por "_"
    out = (
        s.fillna("")
        .astype("string")
        .str.lower()
        .str.strip()
        .str.replace(r"[-\s]+", "_", regex=True)
    )
    # mapear sinônimos
    out = out.replace(TYPE_SYNONYMS)
    # alguns lixos frequentes: "location" -> NaN (será filtrado)
    out = out.replace({"location": pd.NA, "": pd.NA})
    return out.astype("string")


def validate_silver_minimal(**context):
    """
    - Carrega silver da partição do run_date (se existir) ou do diretório inteiro.
    - Normaliza brewery_type.
    - Descarta linhas com tipos inválidos e loga um resumo.
    - Valida com Pandera (deve passar).
    """
    run_date: str = context["ds"]  # YYYY-MM-DD
    df = _load_silver_df(run_date)

    # normalização + limpeza
    df["brewery_type"] = _normalize_brewery_type(df["brewery_type"])

    # filtrar inválidos (fora da whitelist)
    invalid_mask = ~df["brewery_type"].isin(ALLOWED_BREWERY_TYPES)
    invalid_count = int(invalid_mask.sum())
    if invalid_count > 0:
        uniques = (
            df.loc[invalid_mask, "brewery_type"]
            .dropna()
            .unique()
            .tolist()
        )
        print(
            f"[quality] removendo {invalid_count} linhas com brewery_type inválido: {uniques}"
        )
        df = df.loc[~invalid_mask].copy()

    # validação
    SilverSchema.validate(df, lazy=True)
    print(
        f"[quality] OK: {len(df)} linhas válidas em "
        f"{_scan_root_for(run_date)} (run_date={run_date})"
    )
