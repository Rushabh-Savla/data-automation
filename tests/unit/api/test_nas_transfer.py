import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from app.main import app
from app.core.security import require_api_key
from app.api.deps import get_db
from app.schemas.nas import NasConfigResponse, NasTransferResponse
from app.db.models.nas_config import NasConfig

client = TestClient(app)

@pytest.fixture(autouse=True)
def override_deps():
    app.dependency_overrides[require_api_key] = lambda: "test-key"
    mock_db = MagicMock()
    app.dependency_overrides[get_db] = lambda: mock_db
    yield
    app.dependency_overrides.clear()



def test_transfer_unconfigured_nas(monkeypatch):
    monkeypatch.setattr("app.services.nas_config_service.NasConfigService.get", lambda self: None)
    response = client.post(
        "/api/v1/nas/transfer",
        headers={"X-API-Key": "test-key"},
        json={"direction": "local_to_nas", "source_folder": "work_data", "destination_folder": "data_automation"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is False
    assert "No NAS is configured" in data["message"]


def test_transfer_local_to_nas(monkeypatch, tmp_path):
    fake_config = NasConfig(
        protocol="local",
        share=str(tmp_path / "nas_share"),
        storage_path="data_automation",
        base_path="",
        use_as_storage=True,
        auth_mode="anonymous",
    )
    (tmp_path / "nas_share").mkdir(parents=True, exist_ok=True)
    local_src = tmp_path / "local_images"
    local_src.mkdir(parents=True, exist_ok=True)
    (local_src / "img1.jpg").write_bytes(b"image data 1")
    (local_src / "img2.png").write_bytes(b"image data 2")
    (local_src / "notes.txt").write_bytes(b"not an image")

    monkeypatch.setattr("app.services.nas_config_service.NasConfigService.get", lambda self: fake_config)

    response = client.post(
        "/api/v1/nas/transfer",
        headers={"X-API-Key": "test-key"},
        json={"direction": "local_to_nas", "source_folder": str(local_src), "destination_folder": "target_folder"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert data["transferred_count"] == 2
    assert "Successfully transferred 2 image(s)" in data["message"]
