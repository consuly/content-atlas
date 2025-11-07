import os
from datetime import datetime

os.environ.setdefault("SKIP_DB_INIT", "1")

from app.api.schemas.shared import ImportHistoryRecord


def test_import_history_record_allows_missing_file_metadata():
    record = ImportHistoryRecord(
        import_id="import-123",
        import_timestamp=datetime.utcnow(),
        file_name=None,
        file_hash=None,
        table_name="marketing_agency_contacts_us",
        source_type=None,
        status="success",
    )

    assert record.file_name is None
    assert record.file_hash is None
