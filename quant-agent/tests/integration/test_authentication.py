from __future__ import annotations

from fastapi.testclient import TestClient

from apps.api.main import app

EXECUTE_HEADERS = {"x-access-password": "test-access-password"}


def _login(client: TestClient, password: str) -> tuple[str, str]:
    response = client.post("/auth/login", json={"password": password})
    assert response.status_code == 200
    csrf = client.cookies.get("quant_csrf")
    assert csrf
    return response.json()["role"], csrf


def test_url_password_is_rejected_and_openapi_requires_authentication() -> None:
    client = TestClient(app)

    rejected = client.get(
        "/dashboard?pwd=test-access-password",
        headers={"x-correlation-id": "rejected-url-password"},
        follow_redirects=False,
    )
    assert rejected.status_code == 400
    assert "URL password authentication is disabled" in rejected.json()["detail"]
    assert rejected.headers["x-correlation-id"] == "rejected-url-password"

    anonymous_docs = client.get(
        "/openapi.json", headers={"x-correlation-id": "anonymous-openapi"}
    )
    assert anonymous_docs.status_code == 401
    assert anonymous_docs.headers["x-correlation-id"] == "anonymous-openapi"
    authenticated_docs = client.get("/openapi.json", headers=EXECUTE_HEADERS)
    assert authenticated_docs.status_code == 200


def test_browser_login_uses_http_only_cookie_and_requires_csrf_for_writes() -> None:
    client = TestClient(app)
    role, csrf = _login(client, "test-access-password")
    assert role == "execute"
    cookie_response = client.post("/auth/login", json={"password": "test-access-password"})
    set_cookie = "; ".join(cookie_response.headers.get_list("set-cookie"))
    csrf = client.cookies.get("quant_csrf")
    assert "quant_session=" in set_cookie
    assert "HttpOnly" in set_cookie
    assert "SameSite=strict" in set_cookie

    dashboard = client.get("/dashboard")
    assert dashboard.status_code == 200
    assert "searchParams.get('pwd')" not in dashboard.text
    assert "x-csrf-token" in dashboard.text

    missing_csrf = client.post("/events/consume?limit=1")
    assert missing_csrf.status_code == 403
    assert missing_csrf.json()["detail"] == "CSRF validation failed"
    accepted = client.post("/events/consume?limit=1", headers={"x-csrf-token": csrf})
    assert accepted.status_code == 200


def test_production_login_cookie_is_secure_by_default(monkeypatch) -> None:
    monkeypatch.setenv("QUANT_AGENT_TEST_MODE", "0")
    monkeypatch.delenv("QUANT_AGENT_COOKIE_SECURE", raising=False)

    response = TestClient(app).post(
        "/auth/login",
        json={"password": "test-access-password"},
    )

    assert response.status_code == 200
    session_cookie = next(
        value
        for value in response.headers.get_list("set-cookie")
        if value.startswith("quant_session=")
    )
    assert "Secure" in session_cookie
    assert "HttpOnly" in session_cookie


def test_read_approve_execute_roles_are_enforced(monkeypatch) -> None:
    monkeypatch.setenv("QUANT_AGENT_READ_PASSWORD", "read-only-password")
    monkeypatch.setenv("QUANT_AGENT_APPROVAL_PASSWORD", "approval-password")
    monkeypatch.setenv("QUANT_AGENT_EXECUTION_PASSWORD", "execution-password")
    monkeypatch.setenv("QUANT_AGENT_AUTH_SIGNING_SECRET", "test-role-signing-secret")

    read_client = TestClient(app)
    read_role, read_csrf = _login(read_client, "read-only-password")
    assert read_role == "read"
    assert read_client.get("/events/pending").status_code == 200
    read_approval = read_client.post(
        "/recommendations/missing/approval",
        json={"decision": "approved", "approver": "reader"},
        headers={"x-csrf-token": read_csrf},
    )
    assert read_approval.status_code == 403
    assert "approve role required" in read_approval.json()["detail"]

    approval_client = TestClient(app)
    approval_role, approval_csrf = _login(approval_client, "approval-password")
    assert approval_role == "approve"
    allowed_approval = approval_client.post(
        "/recommendations/missing/approval",
        json={"decision": "approved", "approver": "approver"},
        headers={"x-csrf-token": approval_csrf},
    )
    assert allowed_approval.status_code == 404
    blocked_execution = approval_client.post(
        "/events/consume?limit=1",
        headers={"x-csrf-token": approval_csrf},
    )
    assert blocked_execution.status_code == 403
    assert "execute role required" in blocked_execution.json()["detail"]

    execute_client = TestClient(app)
    execute_role, execute_csrf = _login(execute_client, "execution-password")
    assert execute_role == "execute"
    assert execute_client.post(
        "/events/consume?limit=1",
        headers={"x-csrf-token": execute_csrf},
    ).status_code == 200
