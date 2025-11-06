from app.domain.queries.agent import _serialize_rows_to_csv


def test_serialize_rows_to_csv_quotes_commas():
    headers = ["advertiser", "revenue"]
    rows = [["Client, Inc.", "12345.67"]]

    csv_data = _serialize_rows_to_csv(headers, rows)

    assert csv_data == 'advertiser,revenue\n"Client, Inc.",12345.67'


def test_serialize_rows_to_csv_escapes_double_quotes():
    headers = ["advertiser", "revenue"]
    rows = [['Agency "Alpha"', "1000"]]

    csv_data = _serialize_rows_to_csv(headers, rows)

    assert csv_data == 'advertiser,revenue\n"Agency ""Alpha""",1000'


def test_serialize_rows_to_csv_handles_empty_rows():
    headers = ["advertiser", "revenue"]
    rows: list[list[str]] = []

    csv_data = _serialize_rows_to_csv(headers, rows)

    assert csv_data == 'advertiser,revenue\n,'
