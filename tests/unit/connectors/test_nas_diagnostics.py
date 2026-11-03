import os

import pytest
from PIL import Image

from app.connectors.nas import NasConnectionParams
from app.connectors.nas.diagnostics import run_connection_test


def _params(root, **kw):
    return NasConnectionParams(protocol="local", share=str(root), **kw)


@pytest.fixture
def share(tmp_path):
    root = tmp_path / "nas"
    (root / "photos").mkdir(parents=True)
    Image.new("RGB", (80, 80)).save(root / "photos" / "one.jpg")
    return root


class TestConnectionTest:
    def test_healthy_share_connects(self, share):
        out = run_connection_test(_params(share))
        assert out.ok
        assert out.latency_ms is not None
        assert "Connected" in out.message

    def test_missing_mount_fails_with_a_message(self, tmp_path):
        out = run_connection_test(_params(tmp_path / "gone"))
        assert not out.ok and "does not exist" in out.message

