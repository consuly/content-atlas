from decimal import Decimal

from app.db.models import coerce_value_for_sql_type


def test_decimal_coercion_preserves_integer_inputs():
    assert coerce_value_for_sql_type(11, "DECIMAL") == 11
    assert coerce_value_for_sql_type("42", "NUMERIC") == 42
    assert coerce_value_for_sql_type(7.0, "DECIMAL") == 7


def test_decimal_coercion_returns_decimal_for_fractional_values():
    result = coerce_value_for_sql_type("11.5", "DECIMAL")
    assert isinstance(result, Decimal)
    assert result == Decimal("11.5")

    result_float = coerce_value_for_sql_type(3.25, "NUMERIC")
    assert isinstance(result_float, Decimal)
    assert result_float == Decimal("3.25")

