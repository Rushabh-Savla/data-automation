import pytest
from pydantic import ValidationError

from app.schemas.nas import NasConfigInput


def _ok(**kw):
    base = dict(protocol="smb", host="nas.local", share="photos")
    return NasConfigInput(**{**base, **kw})


class TestValidNas:
    def test_open_smb_share_is_valid_with_defaults(self):
        cfg = _ok()
        assert cfg.auth_mode == "anonymous" and cfg.storage_path == "data_automation" and cfg.use_as_storage

    def test_password_protected_smb(self):
        cfg = _ok(auth_mode="password", username="bob", password="pw", domain="CORP")
        assert cfg.password_text() == "pw" and cfg.domain == "CORP"

    def test_ftp_allows_anonymous(self):
        assert _ok(protocol="ftp", share="").auth_mode == "anonymous"

    def test_webdav_and_sftp_password(self):
        _ok(protocol="webdav", share="dav", auth_mode="password", username="u", password="p")
        _ok(protocol="sftp", share="", auth_mode="password", username="u", password="p")

    def test_local_needs_no_host(self):
        assert _ok(protocol="local", host="", share="/mnt/nas").host == ""

    def test_nfs_export_without_login(self):
        cfg = _ok(protocol="nfs", share="/export/photos")
        assert cfg.auth_mode == "anonymous"


class TestInvalidNas:
    @pytest.mark.parametrize(
        "kw, fragment",
        [
            (dict(protocol="sftp"), "SFTP always needs"),
            (dict(host=""), "Host is required"),
            (dict(share=""), "Share name is required"),
            (dict(protocol="nfs", share=""), "export path"),
            (dict(protocol="local", host="", share="relative/dir"), "absolute"),
            (dict(protocol="local", host="", share=""), "Mounted folder"),
            (dict(host="smb://nas/photos"), "only the host"),
            (dict(host="nas/photos"), "only the host"),
            (dict(host="my nas"), "only the host"),
            (dict(auth_mode="password"), "User name is required"),
            (dict(base_path="../x"), "'..'"),
            (dict(storage_path="a/../../b"), "'..'"),
            (dict(port=0), "greater than or equal to 1"),
            (dict(port=70000), "less than or equal to 65535"),
        ],
    )
    def test_rejects(self, kw, fragment):
        with pytest.raises(ValidationError, match=fragment):
            _ok(**kw)

    def test_blank_storage_folder_is_fine_when_not_storing(self):
        assert _ok(use_as_storage=False, storage_path="").storage_path == ""


class TestNormalisation:
    def test_open_access_drops_any_submitted_credentials(self):
        cfg = _ok(auth_mode="anonymous", username="bob", password="pw")
        assert cfg.username == "" and cfg.password is None

    def test_nfs_and_local_ignore_auth_mode(self):
        cfg = _ok(protocol="nfs", share="/e", auth_mode="password", username="x", password="y")
        assert cfg.auth_mode == "anonymous" and cfg.username == "" and cfg.password_text() is None

    def test_domain_only_kept_for_smb(self):
        assert _ok(protocol="ftp", share="", domain="CORP").domain == ""

    def test_paths_are_normalised(self):
        cfg = _ok(base_path="/photos//2024/", storage_path="\\out\\dir")
        assert cfg.base_path == "photos/2024" and cfg.storage_path == "out/dir"

    def test_whitespace_is_trimmed(self):
        assert _ok(host="  nas  ", share=" photos ").host == "nas"

    def test_blank_password_means_keep_saved(self):
        assert _ok(auth_mode="password", username="u", password="").password_text() is None
        assert _ok(auth_mode="password", username="u").password_text() is None

    def test_password_is_not_in_repr_or_dump(self):
        cfg = _ok(auth_mode="password", username="u", password="topsecret")
        assert "topsecret" not in repr(cfg)
        assert "topsecret" not in str(cfg.model_dump())
        assert "topsecret" not in cfg.model_dump_json()
