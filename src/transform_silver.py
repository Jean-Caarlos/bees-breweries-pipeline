# src/transform_silver.py
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

# usamos pyarrow para escrita parquet
import pyarrow  # noqa: F401  # apenas para garantir engine disponível
import pyarrow.parquet as pq  # noqa: F401

from src.config import BRONZE_DIR, SILVER_DIR


# -------------------------
# Helpers de normalização
# -------------------------
_STR_COLS = ["country", "state", "city", "brewery_type", "name"]


def _safe_str(x) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _normalize_fields(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # normaliza strings mais usadas; mantém outras colunas int/float/… como estão
    for c in _STR_COLS:
        if c in df.columns:
            df[c] = df[c].map(_safe_str)
    return df


# -------------------------
# Escrita particionada (usada pelos testes)
# -------------------------
def _write_partitioned_parquet(df: pd.DataFrame, out_dir: Path) -> None:
    """
    Escreve o DataFrame em parquet particionado por country/state no diretório out_dir.

    - out_dir/<country>/<state>/part-0.parquet
    - sobrescreve se já existir
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not {"country", "state"}.issubset(df.columns):
        missing = {"country", "state"} - set(df.columns)
        raise ValueError(f"faltam colunas para particionamento: {sorted(missing)}")

    # normaliza mínimo para evitar pastas com espaços/None
    tmp = df.copy()
    tmp["country"] = tmp["country"].map(_safe_str)
    tmp["state"] = tmp["state"].map(_safe_str)

    for (country, state), part in tmp.groupby(["country", "state"], dropna=False, sort=False):
        # saneia nomes de pastas
        country_dir = _sanitize_partition_value(country)
        state_dir = _sanitize_partition_value(state)

        part_dir = out_dir / country_dir / state_dir
        part_dir.mkdir(parents=True, exist_ok=True)

        # sempre salva com o mesmo nome para manter o teste simples
        part_path = part_dir / "part-0.parquet"
        part.to_parquet(part_path, index=False)  # usa engine=pyarrow por padrão


def _sanitize_partition_value(value: str) -> str:
    """
    Remove caracteres problemáticos para path. Mantém espaços (como no teste).
    """
    s = (value or "").strip()
    # evita separadores de path / backslash, dois pontos etc.
    bad = ['\\', '/', ':', '*', '?', '"', '<', '>', '|']
    for ch in bad:
        s = s.replace(ch, "_")
    return s or "unknown"


# -------------------------
# Leitura do bronze
# -------------------------
def _load_bronze_as_df(base_dir: Path = BRONZE_DIR) -> pd.DataFrame:
    """
    Lê todos os arquivos do bronze daquele dia (json/jsonl) e concatena em um DataFrame.
    Espera arquivos em BRONZE_DIR/ingestion_date=YYYY-MM-DD/*.json(l)
    """
    base_dir = Path(base_dir)
    if not base_dir.exists():
        raise FileNotFoundError(f"Pasta do bronze não encontrada: {base_dir}")

    files = sorted(list(base_dir.rglob("*.jsonl"))) + sorted(list(base_dir.rglob("*.json")))
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo JSON/JSONL encontrado em {base_dir}")

    parts: List[pd.DataFrame] = []
    for f in files:
        if f.suffix == ".jsonl":
            parts.append(pd.read_json(f, lines=True))
        else:
            parts.append(pd.read_json(f))
    return pd.concat(parts, ignore_index=True)


# -------------------------
# Orquestração Silver (usado na DAG)
# -------------------------
def run_silver() -> str:
    """
    Carrega o bronze do dia, normaliza campos e escreve o silver particionado por country/state.
    Retorna o diretório do silver do dia.
    """
    df_bronze = _load_bronze_as_df()
    df_silver = _normalize_fields(df_bronze)

    # se quiser limitar colunas, descomente e ajuste:
    # keep_cols = ["id", "name", "brewery_type", "country", "state", "city", "latitude", "longitude"]
    # df_silver = df_silver[[c for c in keep_cols if c in df_silver.columns]]

    _write_partitioned_parquet(df_silver, SILVER_DIR)
    return str(SILVER_DIR)


__all__ = [
    "_write_partitioned_parquet",
    "_normalize_fields",
    "_load_bronze_as_df",
    "run_silver",
]
