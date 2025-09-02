# src/transform_gold.py
import pandas as pd

def aggregate_breweries_by_type_location(df: pd.DataFrame) -> pd.DataFrame:
    # agrupa e ordena (o run_gold usa essa função por baixo dos panos)
    out = (
        df.groupby(["brewery_type", "country", "state"], as_index=False)
          .size()
          .rename(columns={"size": "n_breweries"})
          .sort_values(["country", "state", "brewery_type"])
          .reset_index(drop=True)
    )
    return out
