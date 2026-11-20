"""Tests for GET /api/v1/nas/status."""

from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app, headers={"X-API-Key": "dev-local-key"})


def test_nas_status_not_configured():
    with patch("app.api.v1.nas.NasConfigService.get", return_value=None):
        # Clear cache first
        from app.api.v1.nas import _STATUS_CACHE
        _STATUS_CACHE.clear()

        resp = client.get("/api/v1/nas/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["configured"] is False
        assert data["connected"] is False


def test_nas_status_connected():
    mock_config = MagicMock()
    mock_config.configured = True
    mock_config.protocol = "sftp"
    mock_config.host = "192.168.1.178"
    mock_config.port = 2222
    mock_config.share = "ai_data_collection"

    with patch("app.api.v1.nas.NasConfigService.get", return_value=mock_config):
        with patch("socket.create_connection", return_value=MagicMock()):
            from app.api.v1.nas import _STATUS_CACHE
            _STATUS_CACHE.clear()

            resp = client.get("/api/v1/nas/status")
            assert resp.status_code == 200
            data = resp.json()
            assert data["configured"] is True
            assert data["connected"] is True
            assert data["protocol"] == "sftp"
            assert data["host"] == "192.168.1.178"
            assert data["port"] == 2222
            assert data["latency_ms"] is not None


def test_nas_status_disconnected():
    mock_config = MagicMock()
    mock_config.configured = True
    mock_config.protocol = "sftp"
    mock_config.host = "192.168.1.178"
    mock_config.port = 2222

    with patch("app.api.v1.nas.NasConfigService.get", return_value=mock_config):
        with patch("socket.create_connection", side_effect=OSError("Connection refused")):
            from app.api.v1.nas import _STATUS_CACHE
            _STATUS_CACHE.clear()

            resp = client.get("/api/v1/nas/status")
            assert resp.status_code == 200
            data = resp.json()
            assert data["configured"] is True
            assert data["connected"] is False
            assert "Connection refused" in data["message"]
