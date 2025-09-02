import json
from pathlib import Path
from datetime import datetime
from .config import BRONZE_DIR, INGESTION_DATE
from .utils import get_with_retry

BASE = "https://api.openbrewerydb.org/v1/breweries"

def fetch_all_breweries(per_page=200, max_pages=200) -> list[dict]:
    all_rows = []
    for page in range(1, max_pages+1):
        data = get_with_retry(BASE, params={"per_page": per_page, "page": page})
        if not data:
            break
        all_rows.extend(data)
        if len(data) < per_page:
            break
    return all_rows

def write_bronze_jsonl(rows: list[dict]) -> Path:
    out_dir = BRONZE_DIR / f"ingestion_date={INGESTION_DATE}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"breweries_{datetime.utcnow().strftime('%H%M%S')}.jsonl"
    with out_path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return out_path

def run_extract():
    rows = fetch_all_breweries()
    return str(write_bronze_jsonl(rows))
