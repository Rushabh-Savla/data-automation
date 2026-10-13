"""SftpClient.connect() behavior around a slow/degraded embedded sshd's SSH
banner handshake — see app/connectors/nas/sftp_client.py."""

from unittest.mock import MagicMock

import paramiko
import pytest

from app.connectors.nas import NasConnectionError, NasConnectionParams
from app.connectors.nas.sftp_client import SftpClient


@pytest.fixture
def params(tmp_path):
    return NasConnectionParams(
        protocol="sftp", host="192.168.1.178", port=2222, share="data", username="Annotation", password="secret"
    )


@pytest.fixture
def client(params, tmp_path, monkeypatch):
    monkeypatch.setattr("app.connectors.nas.sftp_client.get_settings", lambda: _settings(tmp_path))
    return SftpClient(params)


def _settings(tmp_path):
    from types import SimpleNamespace

    return SimpleNamespace(work_root=tmp_path, nas_sftp_max_concurrency=1)


class TestBannerRetry:
    def test_a_transient_banner_failure_is_retried_once_and_recovers(self, client, monkeypatch):
        attempts = []

        def fake_connect(host, **kwargs):
            attempts.append(kwargs)
            if len(attempts) == 1:
                raise paramiko.SSHException("Error reading SSH protocol banner")

        fake_ssh = MagicMock()
        fake_ssh.connect.side_effect = fake_connect
        fake_ssh.open_sftp.return_value.stat.return_value = None
        monkeypatch.setattr(paramiko, "SSHClient", lambda: fake_ssh)

        client.connect()

        assert len(attempts) == 2, "must retry exactly once on a banner failure"

    def test_a_banner_failure_that_never_recovers_gives_an_actionable_message(self, client, monkeypatch):
        fake_ssh = MagicMock()
        fake_ssh.connect.side_effect = paramiko.SSHException("Error reading SSH protocol banner")
        monkeypatch.setattr(paramiko, "SSHClient", lambda: fake_ssh)

        with pytest.raises(NasConnectionError, match="did not respond like an SSH server"):
            client.connect()

    def test_a_non_banner_ssh_error_is_not_retried(self, client, monkeypatch):
        attempts = []

        def fake_connect(host, **kwargs):
            attempts.append(kwargs)
            raise paramiko.AuthenticationException("bad password")

        fake_ssh = MagicMock()
        fake_ssh.connect.side_effect = fake_connect
        monkeypatch.setattr(paramiko, "SSHClient", lambda: fake_ssh)

        with pytest.raises(Exception):
            client.connect()
        assert len(attempts) == 1, "a non-banner failure must not be retried"

    def test_the_connection_slot_is_still_released_after_a_failed_connect(self, client, monkeypatch):
        fake_ssh = MagicMock()
        fake_ssh.connect.side_effect = paramiko.SSHException("Error reading SSH protocol banner")
        monkeypatch.setattr(paramiko, "SSHClient", lambda: fake_ssh)

        with pytest.raises(NasConnectionError):
            client.connect()
        assert getattr(client, "_slot", None) is None, "a failed connect must not leak the concurrency slot"
