import pandas as pd

from app.domain.imports.mapper import detect_column_type


def test_detect_column_type_bigint_for_large_ids():
    series = pd.Series([120000000000000000, 2147483648, None])

    inferred = detect_column_type(series)

    assert inferred == "BIGINT"
