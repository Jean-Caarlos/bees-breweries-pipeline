import pandas as pd
from src.transform_gold import aggregate_breweries_by_type_location

def test_gold_aggregation():
    df = pd.DataFrame([
        {"brewery_type":"micro","country":"US","state":"TX"},
        {"brewery_type":"micro","country":"US","state":"TX"},
        {"brewery_type":"brewpub","country":"US","state":"TX"},
        {"brewery_type":"micro","country":"PT","state":"Lisboa"},
    ])
    out = aggregate_breweries_by_type_location(df)
    assert set(out.columns) == {"brewery_type","country","state","n_breweries"}
    assert out.loc[(out.country=="US") & (out.state=="TX") & (out.brewery_type=="micro"), "n_breweries"].item() == 2
    assert out.loc[(out.country=="US") & (out.state=="TX") & (out.brewery_type=="brewpub"), "n_breweries"].item() == 1
    assert out.loc[(out.country=="PT") & (out.state=="Lisboa") & (out.brewery_type=="micro"), "n_breweries"].item() == 1
