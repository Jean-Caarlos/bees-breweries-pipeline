import pandas as pd
import pytest
from pandera.errors import SchemaError
from src.quality import SilverSchema

def test_silver_schema_ok():
    df = pd.DataFrame([{
        "id": 1, "name": "Brew 1", "brewery_type": "micro",
        "country": "United States", "state": "Texas", "city": "Austin"
    }])
    SilverSchema.validate(df)  # não deve levantar exceção

def test_silver_schema_fail():
    df = pd.DataFrame([{
        "id": 1, "name": "Brew 1", "brewery_type": None,  # <- inválido
        "country": "United States", "state": "Texas", "city": "Austin"
    }])
    with pytest.raises(SchemaError):
        SilverSchema.validate(df)
