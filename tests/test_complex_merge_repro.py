
import io
import time
import zipfile
from typing import Dict
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from app.main import app
from tests.utils.system_tables import ensure_system_tables_ready

client = TestClient(app)

CSV_CONTENT_A = """"Research Date","Contact Full Name","First Name","Middle Name","Last Name","Title","Department","Seniority","Company Name","Company Name - Cleaned","Website","List","Primary Email","Intel","Contact LI Profile URL","Email 1","Email 1 Validation","Email 1 Total AI","Email 2","Email 2 Validation","Email 2 Total AI","Email 3","Email 3 Validation","Email 3 Total AI","Email 4","Email 4 Validation","Email 4 Total AI","Email 5","Email 5 Validation","Email 5 Total AI","Email 6","Email 6 Validation","Email 6 Total AI","Email 7","Email 7 Validation","Email 7 Total AI","Email 8","Email 8 Validation","Email 8 Total AI","Email 9","Email 9 Validation","Email 9 Total AI","Email 10","Email 10 Validation","Email 10 Total AI","Personal Email","Personal Email Validation","Personal Email Total AI","Personal Email 2","Personal Email 2 Validation","Personal Email 2 Total AI","Personal Email 3","Personal Email 3 Validation","Personal Email 3 Total AI","Contact Phone 1","Contact Phone 1 Total AI","Company Phone 1","Company Phone 1 Total AI","Contact Phone 2","Contact Phone 2 Total AI","Company Phone 2","Company Phone 2 Total AI","Contact Phone 3","Contact Phone 3 Total AI","Company Phone 3","Company Phone 3 Total AI","Contact Phone 4","Contact Phone 4 Total AI","Company Phone 4","Company Phone 4 Total AI","Contact Phone 5","Contact Phone 5 Total AI","Company Phone 5","Company Phone 5 Total AI","Contact Phone 6","Contact Phone 6 Total AI","Company Phone 6","Company Phone 6 Total AI","Contact Phone 7","Contact Phone 7 Total AI","Company Phone 7","Company Phone 7 Total AI","Contact Phone 8","Contact Phone 8 Total AI","Company Phone 8","Company Phone 8 Total AI","Contact Phone 9","Contact Phone 9 Total AI","Company Phone 9","Company Phone 9 Total AI","Contact Phone 10","Contact Phone 10 Total AI","Company Phone 10","Company Phone 10 Total AI","Contact Location","Contact City","Contact State","Contact State Abbr","Contact Post Code","Contact County","Contact Country","Contact Country (Alpha 2)","Contact Country (Alpha 3)","Contact Country - Numeric","Company Location","Company Street 1","Company Street 2","Company Street 3","Company City","Company State","Company State Abbr","Company Post Code","Company County","Company Country","Company Country (Alpha 2)","Company Country (Alpha 3)","Company Country - Numeric","Company Annual Revenue","Company Description","Company Website Domain","Company Founded Date","Company Industry","Company LI Profile Url","Company LinkedIn ID","Company Revenue Range","Company Staff Count","Company Staff Count Range","Seamless Username","CRM & Social","CRM Account ID","Contact Location - City","Contact Location - Country","Contact Location - Country Alpha-2 Code","Contact Location - Country Alpha-3 Code","Contact Location - Country Numeric Code","Contact Location - State","Contact Location - State Abbreviation","Contact Location - ZIP","Contact Phone","Date Imported","First Name","Last Name","Lists","Location","leadSource"
"2024-06-04T13:00:03.014Z","Jenny Clarke","Jenny","","Clarke","Co-Founder & Digital Director","IT","C-Level","Bright Sprout","Bright Sprout","brightsprout.co.uk","Marketing Agency, 1 - 11E, O/F/C, US","jenny@brightsprout.co.uk","https://login.seamless.ai/contact/4951289310","https://www.linkedin.com/in/jenny-clarke-marketing","jenny@brightsprout.co.uk","accept all","68%","jenny.clarke@brightsprout.co.uk","accept all","34%","jclarke@brightsprout.co.uk","accept all","33%","","","","","","","","","","","","","","","","","","","","","","jenny.clarke2011@gmail.com","valid","93%","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","Cardiff, United States","Cardiff","","","","","United States","US","USA","840","","","","","","","","","","","","","","","Bright Sprout is a creative & digital marketing agency, providing services across Bristol, Somerset and South Wales. We're passionate about helping businesses grow and reach their full potential!","brightsprout.co.uk","","Advertising Services","https://www.linkedin.com/company/71678219","71678219","","1001","1,001 - 5,000 employees","","","","","","","","","","","","","","","","","",""
"2024-06-04T13:00:02.701Z","Bill Walls","Bill","","Walls","Owner","Other","C-Level","InTouch Marketing","InTouch Marketing","intouch-marketing.com","Marketing Agency, 1 - 11E, O/F/C, US","billw@intouch-marketing.com","https://login.seamless.ai/contact/4951289300","https://www.linkedin.com/in/intouchmk","billw@intouch-marketing.com","valid","98%","info@intouch-marketing.com","do not mail","92%","bwalls@intouchmarketing.us","invalid","14%","","","","","","","","","","","","","","","","","","","","","","billintouchmk@gmail.com","valid","93%","billwalls@roadrunner.com","invalid","15%","","","","909.730.2819","12%","909.392.2164","99%","+44-1295-261161","2%","","","669.900.6833","2%","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","","La Verne, CA, United States","La Verne","California","CA","","","United States","US","USA","840","23779 Yellowbill Terrace, Moreno Valley, CA 92557, United States","23779 Yellowbill Terrace","","","Moreno Valley","California","CA","92557","","United States","US","USA","840","10000000","InTouch Marketing provides lead generation strategies for businesses through the use of Inbound Marketing, PPC Management, Social Media, Link Building, Web Design & SEO.","intouch-marketing.com","","Marketing and Advertising","https://www.linkedin.com/company/3353339","3353339","$5M - $20M","10","2 - 10 employees","","","","","","","","","","","","909.392.2164","","","","","",""
"""

CSV_CONTENT_B = CSV_CONTENT_A  # Identical content

def _build_zip(file_map: Dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        for archive_name, content in file_map.items():
            zf.writestr(archive_name, content)
    return buffer.getvalue()

def _upload_zip(zip_bytes: bytes, filename: str = "batch.zip") -> str:
    response = client.post(
        "/upload-to-b2",
        data={"allow_duplicate": "true"},
        files={"file": (filename, io.BytesIO(zip_bytes), "application/zip")},
    )
    assert response.status_code == 200, response.text
    return response.json()["files"][0]["id"]

def _wait_for_job(job_id: str, timeout: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        resp = client.get(f"/import-jobs/{job_id}")
        assert resp.status_code == 200
        job = resp.json().get("job")
        if job and job.get("status") in ("succeeded", "failed"):
            return job
        time.sleep(1)
    raise AssertionError(f"Job {job_id} timed out")

@pytest.fixture(autouse=True)
def reset_tables():
    engine = ensure_system_tables_ready()
    with engine.begin() as conn:
        for table in ("marketing_contacts", "marketing_agencies", "contacts", "complex_contacts"):
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
        if file_path not in storage:
            # Check if it was uploaded via upload endpoint which might use different path
            # But here we control the path via fake_upload
            pass
        return storage.get(file_path, b"")
        
    monkeypatch.setattr("app.api.routers.uploads.upload_file_to_storage", fake_upload)
    monkeypatch.setattr("app.integrations.storage.download_file", fake_download)
    monkeypatch.setattr("app.api.routers.analysis.routes._download_file_from_storage", fake_download)
    return storage

def test_complex_merge_repro(fake_storage, monkeypatch):
    # Disable retry for speed
    monkeypatch.setattr("app.core.config.settings.enable_auto_retry_failed_imports", False, raising=False)

    zip_bytes = _build_zip({
        "complex_contacts_a.csv": CSV_CONTENT_A,
        "complex_contacts_b.csv": CSV_CONTENT_B
    })
    archive_id = _upload_zip(zip_bytes, filename="complex_batch.zip")

    response = client.post(
        "/auto-process-archive",
        data={
            "file_id": archive_id,
            "analysis_mode": "auto_always",
            "conflict_resolution": "llm_decide",
            "max_iterations": "5",
            "llm_instruction": "Keep only the primary email and phone number",
        },
    )
    assert response.status_code == 200
    job_id = response.json()["job_id"]

    job = _wait_for_job(job_id)
    assert job["status"] == "succeeded", f"Job failed: {job.get('error_message')}"

    results = job["result_metadata"]["results"]
    processed = [r for r in results if r["status"] == "processed"]
    assert len(processed) == 2
    
    table_names = {r["table_name"] for r in processed}
    print(f"Created tables: {table_names}")
    
    # Reproduction criteria: if len(table_names) > 1, the bug is reproduced
    assert len(table_names) == 1, f"Should have merged into one table, but created: {table_names}"

    # Verify table_fingerprints is populated
    from app.db.session import get_engine
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM table_fingerprints"))
        count = result.scalar()
        assert count == 1, f"Should have stored 1 fingerprint, got {count}"
        
        table_name = list(table_names)[0]
        result = conn.execute(text("SELECT table_name FROM table_fingerprints WHERE table_name = :table"), {"table": table_name})
        assert result.scalar() == table_name
