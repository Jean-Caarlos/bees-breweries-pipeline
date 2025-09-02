import pandas as pd
from pathlib import Path

from src.transform_silver import _write_partitioned_parquet

def test_write_partitioned_parquet(tmp_path: Path):
    df = pd.DataFrame([
        {"country": "United States", "state": "Texas", "brewery_type": "micro", "city": "Austin"},
        {"country": "United States", "state": "Texas", "brewery_type": "brewpub", "city": "Austin"},
        {"country": "Portugal", "state": "Lisboa", "brewery_type": "micro", "city": "Lisboa"},
    ])

    out = tmp_path / "silver"
    _write_partitioned_parquet(df, out)  # deve criar partições country/state

    assert (out / "United States" / "Texas" / "part-0.parquet").exists()
    assert (out / "Portugal" / "Lisboa" / "part-0.parquet").exists()
