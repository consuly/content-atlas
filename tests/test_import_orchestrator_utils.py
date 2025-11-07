from app.domain.imports.orchestrator import _records_look_like_mappings


def test_records_validator_accepts_dict_like_sequences():
    assert _records_look_like_mappings([{"a": 1}, {"b": 2}]) is True
    assert _records_look_like_mappings([None, {"c": 3}]) is True


def test_records_validator_rejects_strings_and_lists():
    assert _records_look_like_mappings(["row as string"]) is False
    assert _records_look_like_mappings([["cell_1", "cell_2"]]) is False
    assert _records_look_like_mappings("totally invalid") is False
