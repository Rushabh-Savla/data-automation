from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from app.connectors.nas.base import RemoteEntry
from app.main import app

client = TestClient(app, headers={"X-API-Key": "dev-local-key"})


def test_list_nas_folders_unconfigured():
    with patch("app.api.v1.nas.NasConfigService.get", return_value=None):
        r = client.get("/api/v1/nas/folders")
        assert r.status_code == 404


def test_list_nas_folders_success():
    mock_saved = MagicMock()
    mock_client = MagicMock()
    mock_client.list_dir.return_value = [
        RemoteEntry(name="folder1", is_dir=True, size=0, mtime=0.0),
        RemoteEntry(name="file.txt", is_dir=False, size=100, mtime=0.0),
        RemoteEntry(name="folder2", is_dir=True, size=0, mtime=0.0),
        RemoteEntry(name=".hidden", is_dir=True, size=0, mtime=0.0),
    ]

    with patch("app.api.v1.nas.NasConfigService.get", return_value=mock_saved), \
         patch("app.api.v1.nas.NasConfigService.params_for", return_value=MagicMock()), \
         patch("app.api.v1.nas.open_client") as mock_open:
        mock_open.return_value.__enter__.return_value = mock_client
        r = client.get("/api/v1/nas/folders?path=ai_data_collection")
        assert r.status_code == 200
        data = r.json()
        assert data["current_path"] == "ai_data_collection"
        assert data["folders"] == ["folder1", "folder2"]


def test_verify_nas_folder_exists():
    mock_saved = MagicMock()
    mock_client = MagicMock()
    mock_client.list_dir.return_value = []

    with patch("app.api.v1.nas.NasConfigService.get", return_value=mock_saved), \
         patch("app.api.v1.nas.NasConfigService.params_for", return_value=MagicMock()), \
         patch("app.api.v1.nas.open_client") as mock_open:
        mock_open.return_value.__enter__.return_value = mock_client
        r = client.post("/api/v1/nas/verify-folder", json={"folder_path": "ai_data_collection/test"})
        assert r.status_code == 200
        data = r.json()
        assert data["exists"] is True


def test_verify_nas_folder_not_found():
    mock_saved = MagicMock()
    mock_client = MagicMock()
    mock_client.list_dir.side_effect = Exception("Not found")

    with patch("app.api.v1.nas.NasConfigService.get", return_value=mock_saved), \
         patch("app.api.v1.nas.NasConfigService.params_for", return_value=MagicMock()), \
         patch("app.api.v1.nas.open_client") as mock_open:
        mock_open.return_value.__enter__.return_value = mock_client
        r = client.post("/api/v1/nas/verify-folder", json={"folder_path": "missing/folder"})
        assert r.status_code == 200
        data = r.json()
        assert data["exists"] is False

