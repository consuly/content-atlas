"""
Regression tests for API key authentication behaviors.
"""

import os
from datetime import datetime, timedelta

os.environ["SKIP_DB_INIT"] = "1"

from app.core.api_key_auth import ApiKey, hash_api_key, verify_api_key  # noqa: E402


class _FakeQuery:
    def __init__(self, record):
        self.record = record

    def filter(self, *args, **kwargs):
        return self

    def first(self):
        return self.record


class _FakeSession:
    def __init__(self, record):
        self.record = record

    def query(self, *_):
        return _FakeQuery(self.record)


def test_verify_api_key_handles_naive_future_expiration():
    """Naive future expires_at should not raise TypeError and returns the record."""
    plain_key = "atlas_live_sk_test"
    record = ApiKey(
        id="test",
        key_hash=hash_api_key(plain_key),
        app_name="test-app",
        is_active=True,
        expires_at=datetime.utcnow() + timedelta(days=1),
    )

    db = _FakeSession(record)
    assert verify_api_key(db, plain_key) is record


def test_verify_api_key_rejects_naive_expired_key():
    """Naive past expires_at should be treated as expired instead of erroring."""
    plain_key = "atlas_live_sk_test_expired"
    record = ApiKey(
        id="test-expired",
        key_hash=hash_api_key(plain_key),
        app_name="test-app",
        is_active=True,
        expires_at=datetime.utcnow() - timedelta(days=1),
    )

    db = _FakeSession(record)
    assert verify_api_key(db, plain_key) is None
