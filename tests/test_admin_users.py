from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.main import app
from app.db.session import get_engine
from app.core.security import create_access_token, create_user, delete_user

client = TestClient(app)


@pytest.fixture
def user_factory():
    """
    Helper fixture that creates users directly via the ORM and cleans them up.
    Returns a callable so tests can create both admin and standard users.
    """
    engine = get_engine()
    session = Session(engine)
    created_ids: list[int] = []

    def _create_user(*, role: str = "user", password: str = "Password123!") -> dict:
        email = f"{role}_{uuid4().hex}@example.com"
        user = create_user(
            db=session,
            email=email,
            password=password,
            full_name="Pytest User",
            role=role,
        )
        created_ids.append(user.id)
        token = create_access_token({"sub": user.email})
        return {"user": user, "token": token, "email": email, "password": password}

    yield _create_user

    for user_id in created_ids:
        delete_user(session, user_id)
    session.close()


def test_admin_can_create_reset_and_delete_users(user_factory):
    admin = user_factory(role="admin")
    admin_headers = {"Authorization": f"Bearer {admin['token']}"}

    managed_email = f"managed_{uuid4().hex}@example.com"
    initial_password = "InitialPass123!"
    new_password = "UpdatedPass123!"
    managed_user_id = None
    deleted_via_api = False

    try:
        create_response = client.post(
            "/admin/users",
            headers=admin_headers,
            json={
                "email": managed_email,
                "password": initial_password,
                "full_name": "Managed User",
                "role": "user",
            },
        )
        assert create_response.status_code == 200
        payload = create_response.json()
        managed_user = payload["user"]
        managed_user_id = managed_user["id"]
        assert managed_user["email"] == managed_email
        assert managed_user["role"] == "user"

        list_response = client.get("/admin/users", headers=admin_headers)
        assert list_response.status_code == 200
        emails = [user["email"] for user in list_response.json()["users"]]
        assert managed_email in emails

        reset_response = client.patch(
            f"/admin/users/{managed_user_id}/password",
            headers=admin_headers,
            json={"password": new_password},
        )
        assert reset_response.status_code == 200
        assert reset_response.json()["user"]["id"] == managed_user_id

        login_response = client.post(
            "/auth/login", json={"email": managed_email, "password": new_password}
        )
        assert login_response.status_code == 200

        delete_response = client.delete(
            f"/admin/users/{managed_user_id}", headers=admin_headers
        )
        assert delete_response.status_code == 200
        assert delete_response.json()["deleted_user_id"] == managed_user_id
        deleted_via_api = True

        failed_login = client.post(
            "/auth/login", json={"email": managed_email, "password": new_password}
        )
        assert failed_login.status_code == 401
    finally:
        if managed_user_id and not deleted_via_api:
            engine = get_engine()
            with Session(engine) as cleanup_session:
                delete_user(cleanup_session, managed_user_id)


def test_standard_user_cannot_access_admin_user_routes(user_factory):
    admin = user_factory(role="admin")
    standard_user = user_factory(role="user")
    user_headers = {"Authorization": f"Bearer {standard_user['token']}"}

    forbidden_get = client.get("/admin/users", headers=user_headers)
    assert forbidden_get.status_code == 403

    forbidden_create = client.post(
        "/admin/users",
        headers=user_headers,
        json={
            "email": f"blocked_{uuid4().hex}@example.com",
            "password": "Nope1234!",
            "role": "user",
        },
    )
    assert forbidden_create.status_code == 403

    forbidden_reset = client.patch(
        f"/admin/users/{admin['user'].id}/password",
        headers=user_headers,
        json={"password": "CantTouch123"},
    )
    assert forbidden_reset.status_code == 403

    forbidden_delete = client.delete(
        f"/admin/users/{admin['user'].id}", headers=user_headers
    )
    assert forbidden_delete.status_code == 403

    unauthenticated = client.get("/admin/users")
    assert unauthenticated.status_code == 403


def test_admin_cannot_delete_self(user_factory):
    admin = user_factory(role="admin")
    headers = {"Authorization": f"Bearer {admin['token']}"}

    response = client.delete(f"/admin/users/{admin['user'].id}", headers=headers)
    assert response.status_code == 400
    assert "Cannot delete" in response.json()["detail"]
