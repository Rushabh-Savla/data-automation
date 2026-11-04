from fastapi.testclient import TestClient
from app.main import app


def test_root_endpoint():
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "Data Automation Platform API"
    assert data["status"] == "running"
    assert data["docs"] == "/docs"
    assert data["openapi"] == "/openapi.json"


def test_healthz_endpoint():
    client = TestClient(app)
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_api_v1_health_backward_compat():
    client = TestClient(app)
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_protected_endpoint_without_key_returns_401():
    client = TestClient(app)
    response = client.get("/api/v1/jobs")
    assert response.status_code == 401
    assert response.json()["detail"] == "Missing or invalid API key"


def test_protected_endpoint_with_valid_key_succeeds(db_session, monkeypatch):
    client = TestClient(app)
    # Ensure db_session fixture is available
    from app.api.deps import get_db
    app.dependency_overrides[get_db] = lambda: db_session
    try:
        response = client.get("/api/v1/jobs", headers={"X-API-Key": "dev-local-key"})
        assert response.status_code == 200
        assert "items" in response.json()
    finally:
        app.dependency_overrides.pop(get_db, None)
