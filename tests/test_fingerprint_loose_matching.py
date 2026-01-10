
import io
import time
import zipfile
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from app.main import app
from tests.utils.system_tables import ensure_system_tables_ready

client = TestClient(app)

# Original complex content
CSV_CONTENT_A = """"Research Date","Contact Full Name","First Name","Middle Name","Last Name","Title","Department","Seniority","Company Name","Company Name - Cleaned","Website","List","Primary Email","Intel","Contact LI Profile URL","Email 1","Email 1 Validation","Email 1 Total AI","Email 2","Email 2 Validation","Email 2 Total AI","Email 3","Email 3 Validation","Email 3 Total AI","Email 4","Email 4 Validation","Email 4 Total AI","Email 5","Email 5 Validation","Email 5 Total AI","Email 6","Email 6 Validation","Email 6 Total AI","Email 7","Email 7 Validation","Email 7 Total AI","Email 8","Email 8 Validation","Email 8 Total AI","Email 9","Email 9 Validation","Email 9 Total AI","Email 10","Email 10 Validation","Email 10 Total AI","Personal Email","Personal Email Validation","Personal Email Total AI","Personal Email 2","Personal Email 2 Validation","Personal Email 2 Total AI","Personal Email 3","Personal Email 3 Validation","Personal Email 3 Total AI","Contact Phone 1","Contact Phone 1 Total AI","Company Phone 1","Company Phone 1 Total AI","Contact Phone 2","Contact Phone 2 Total AI","Company Phone 2","Company Phone 2 Total AI","Contact Phone 3","Contact Phone 3 Total AI","Company Phone 3","Company Phone 3 Total AI","Contact Phone 4","Contact Phone 4 Total AI","Company Phone 4","Company Phone 4 Total AI","Contact Phone 5","Contact Phone 5 Total AI","Company Phone 5","Company Phone 5 Total AI","Contact Phone 6","Contact Phone 6 Total AI","Company Phone 6","Company Phone 6 Total AI","Contact Phone 7","Contact Phone 7 Total AI","Company Phone 7","Company Phone 7 Total AI","Contact Phone 8","Contact Phone 8 Total AI","Company Phone 8","Company Phone 8 Total AI","Contact Phone 9","Contact Phone 9 Total AI","Company Phone 9","Company Phone 9 Total AI","Contact Phone 10","Contact Phone 10 Total AI","Company Phone 10","Company Phone 10 Total AI","Contact Location","Contact City","Contact State","Contact State Abbr","Contact Post Code","Contact County","Contact Country","Contact Country (Alpha 2)","Contact Country (Alpha 3)","Contact Country - Numeric","Company Location","Company Street 1","Company Street 2","Company Street 3","Company City","Company State","Company State Abbr","Company Post Code","Company County","Company Country","Company Country (Alpha 2)","Company Country (Alpha 3)","Company Country - Numeric","Company Annual Revenue","Company Description","Company Website Domain","Company Founded Date","Company Industry","Company LI Profile Url","Company LinkedIn ID","Company Revenue Range","Company Staff Count","Company Staff Count Range","Seamless Username","CRM & Social","CRM Account ID","Contact Location - City","Contact Location - Country","Contact Location - Country Alpha-2 Code","Contact Location - Country Alpha-3 Code","Contact Location - Country Numeric Code","Contact Location - State","Contact Location - State Abbreviation","Contact Location - ZIP","Contact Phone","Date Imported","First Name","Last Name","Lists","Location","leadSource"
"2024-06-04T13:00:03.014Z","Jenny Clarke","Jenny","","Clarke","Co-Founder & Digital Director","IT","C-Level","Bright Sprout","Bright Sprout","brightsprout.co.uk","Marketing Agency, 1 - 11E, O/F/C, US","jenny@brightsprout.co.uk","https://login.seamless.ai/contact/4951289310","https://www.linkedin.com/in/jenny-clarke-marketing","jenny@brightsprout.co.uk","accept all","68%","jenny.clarke@brightsprout.co.uk","accept all","34%","jclarke@brightsprout.co.uk","accept all","33%","","","","","","","","","","","","","","","","","","","","","","jenny.clarke2011@gmail.com","valid","93%","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","Cardiff, United States","Cardiff","","","","","United States","US","USA","840","","","","","","","","","","","","","","","Bright Sprout is a creative & digital marketing agency, providing services across Bristol, Somerset and South Wales. We're passionate about helping businesses grow and reach their full potential!","brightsprout.co.uk","","Advertising Services","https://www.linkedin.com/company/71678219","71678219","","1001","1,001 - 5,000 employees","","","","","","","","","","","","","","","","","",""
"""

# Slightly modified content (one extra column)
# This simulates a "loose match" (90%+ similarity)
CSV_CONTENT_B = CSV_CONTENT_A.replace(
    '"Research Date","Contact Full Name"', 
    '"Research Date","Extra Column","Contact Full Name"'
).replace(
    '"2024-06-04T13:00:03.014Z","Jenny Clarke"',
    '"2024-06-04T13:00:03.014Z","Extra Value","Jenny Clarke"'
)

def _build_zip(files):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buffer.getvalue()

def _upload_zip(zip_bytes, filename="batch.zip", headers=None):
    response = client.post(
        "/upload-to-b2",
        data={"allow_duplicate": "true"},
        files={"file": (filename, io.BytesIO(zip_bytes), "application/zip")},
        headers=headers,
    )
    assert response.status_code == 200
    return response.json()["files"][0]["id"]

def _wait_for_job(job_id, headers=None):
    for _ in range(60):
        resp = client.get(f"/import-jobs/{job_id}", headers=headers)
        job = resp.json().get("job")
        if job and job.get("status") in ("succeeded", "failed"):
            return job
        time.sleep(0.5)
    raise AssertionError("Job timed out")

@pytest.fixture(autouse=True)
def reset_tables():
    engine = ensure_system_tables_ready()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM table_fingerprints"))
        # Drop likely tables
        for table in ("marketing_contacts", "contacts"):
            conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE'))
    yield

@pytest.fixture
def fake_storage(monkeypatch):
    storage = {}
    def fake_upload(file_content, file_name, folder="uploads"):
        path = f"{folder}/{file_name}"
        storage[path] = bytes(file_content)
        return {"file_id": file_name, "file_path": path, "size": len(file_content)}
    def fake_download(file_path):
        return storage.get(file_path, b"")
    
    monkeypatch.setattr("app.api.routers.uploads.upload_file_to_storage", fake_upload)
    monkeypatch.setattr("app.integrations.storage.download_file", fake_download)
    monkeypatch.setattr("app.api.routers.analysis.routes._download_file_from_storage", fake_download)
    return storage

def test_loose_matching_merges_tables(fake_storage, monkeypatch, auth_headers):
    """
    Test that a file with >90% similarity to an existing table is merged into it.
    
    1. Import File A (original).
    2. Import File B (original + 1 column).
    3. Verify both go to the same table.
    """
    monkeypatch.setattr("app.core.config.settings.enable_auto_retry_failed_imports", False, raising=False)

    # Step 1: Import File A
    zip_a = _build_zip({"file_a.csv": CSV_CONTENT_A})
    id_a = _upload_zip(zip_a, "file_a.zip", headers=auth_headers)
    
    resp_a = client.post("/auto-process-archive", data={
        "file_id": id_a,
        "analysis_mode": "auto_always",
        "conflict_resolution": "llm_decide",
        "llm_instruction": "Keep only the primary email and phone number", # Simplify schema
    }, headers=auth_headers)
    job_a = _wait_for_job(resp_a.json()["job_id"], headers=auth_headers)
    assert job_a["status"] == "succeeded"
    
    results_a = job_a["result_metadata"]["results"]
    table_a = results_a[0]["table_name"]
    print(f"File A created table: {table_a}")

    # Verify fingerprint stored
    from app.db.session import get_engine
    engine = get_engine()
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM table_fingerprints WHERE table_name = :t"), {"t": table_a}).scalar()
        assert count == 1, "Fingerprint for Table A should exist"

    # Step 2: Import File B (Loose match)
    zip_b = _build_zip({"file_b.csv": CSV_CONTENT_B})
    id_b = _upload_zip(zip_b, "file_b.zip", headers=auth_headers)
    
    resp_b = client.post("/auto-process-archive", data={
        "file_id": id_b,
        "analysis_mode": "auto_always",
        "conflict_resolution": "llm_decide",
        "llm_instruction": "Keep only the primary email and phone number",
    }, headers=auth_headers)
    job_b = _wait_for_job(resp_b.json()["job_id"], headers=auth_headers)
    assert job_b["status"] == "succeeded"
    
    results_b = job_b["result_metadata"]["results"]
    table_b = results_b[0]["table_name"]
    print(f"File B targeted table: {table_b}")
    
    # Assert they are the same table
    assert table_a == table_b, f"File B should have merged into {table_a}, but went to {table_b}"
