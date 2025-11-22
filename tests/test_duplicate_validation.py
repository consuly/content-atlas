import pytest

from app.db.models import _validate_uniqueness_columns


def test_validate_uniqueness_columns_passes_when_all_present():
    _validate_uniqueness_columns(
        table_name="example",
        uniqueness_columns=["email", "first_name"],
        existing_columns=["id", "email", "first_name", "created_at"],
    )


def test_validate_uniqueness_columns_raises_with_suggestion():
    with pytest.raises(ValueError) as exc:
        _validate_uniqueness_columns(
            table_name="example",
            uniqueness_columns=["email"],
            existing_columns=["emails", "first_name"],
        )

    assert "email" in str(exc.value)
    assert "emails" in str(exc.value)
