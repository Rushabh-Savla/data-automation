import pytest
from cryptography.fernet import Fernet

import app.services.nas_config_service as svc_mod
from app.core.security import CredentialCipher
from app.schemas.nas import NasConfigInput
from app.services.nas_config_service import NasConfigService, NasPasswordRequired

KEY = Fernet.generate_key().decode()


class FakeDb:
    """Just enough of a Session for NasConfigRepository."""

    def __init__(self):
        self.row = None

    def get(self, model, pk):
        return self.row

    def add(self, obj):
        self.row = obj

    def flush(self):
        pass

    def delete(self, obj):
        self.row = None


@pytest.fixture(autouse=True)
def _cipher(monkeypatch):
    monkeypatch.setattr(svc_mod, "CredentialCipher", lambda: CredentialCipher(key=KEY))


def _input(**kw):
    base = dict(protocol="smb", host="nas", share="photos", auth_mode="password", username="bob", password="hunter2")
    return NasConfigInput(**{**base, **kw})


@pytest.fixture
def service():
    return NasConfigService(FakeDb())


def _decrypt(row):
    return CredentialCipher(key=KEY).decrypt(row.encrypted_password)


class TestSave:
    def test_password_is_stored_encrypted_not_plaintext(self, service):
        row = service.save(_input())
        assert row.encrypted_password and b"hunter2" not in row.encrypted_password
        assert _decrypt(row) == "hunter2"

    def test_open_nas_stores_no_password_and_needs_no_encryption_key(self, service, monkeypatch):
        def boom():
            raise RuntimeError("CREDENTIAL_ENCRYPTION_KEY is not set")

        monkeypatch.setattr(svc_mod, "CredentialCipher", boom)
        row = service.save(NasConfigInput(protocol="smb", host="nas", share="open"))
        assert row.encrypted_password is None and row.auth_mode == "anonymous"

    def test_blank_password_on_edit_keeps_the_saved_one(self, service):
        service.save(_input())
        row = service.save(_input(password=None, host="nas2"))
        assert row.host == "nas2" and _decrypt(row) == "hunter2"

    def test_new_password_replaces_the_old(self, service):
        service.save(_input())
        assert _decrypt(service.save(_input(password="new-pass"))) == "new-pass"

    def test_switching_to_open_access_clears_the_password(self, service):
        service.save(_input())
        row = service.save(NasConfigInput(protocol="smb", host="nas", share="photos"))
        assert row.encrypted_password is None

    def test_password_mode_without_any_password_is_refused(self, service):
        with pytest.raises(NasPasswordRequired):
            service.save(_input(password=None))

    def test_saving_twice_keeps_one_row(self, service):
        service.save(_input())
        service.save(_input(host="other"))
        assert service.get().host == "other"

    def test_delete_removes_the_config(self, service):
        service.save(_input())
        service.delete()
        assert service.get() is None


class TestParams:
    def test_params_for_decrypts_the_password(self, service):
        params = service.params_for(service.save(_input()))
        assert params.password == "hunter2" and params.uses_password

    def test_params_for_open_nas_has_no_password(self, service):
        row = service.save(NasConfigInput(protocol="ftp", host="ftp.lan"))
        params = service.params_for(row)
        assert params.password == "" and not params.uses_password

    def test_test_button_with_blank_password_falls_back_to_the_saved_one(self, service):
        service.save(_input())
        assert service.params_from_input(_input(password=None)).password == "hunter2"

    def test_test_button_prefers_a_freshly_typed_password(self, service):
        service.save(_input())
        assert service.params_from_input(_input(password="typed")).password == "typed"

    def test_test_button_before_anything_is_saved(self, service):
        assert service.params_from_input(_input(password=None)).password == ""

    def test_params_repr_hides_the_password(self, service):
        assert "hunter2" not in repr(service.params_for(service.save(_input())))


class TestActiveStorage:
    def _patch_session(self, monkeypatch, row):
        import contextlib

        @contextlib.contextmanager
        def fake_scope():
            db = FakeDb()
            db.row = row
            yield db

        monkeypatch.setattr(svc_mod, "session_scope", fake_scope)

    def test_none_when_unconfigured(self, monkeypatch):
        self._patch_session(monkeypatch, None)
        assert svc_mod.get_active_storage_params() is None
        assert svc_mod.get_saved_params() is None
        assert not svc_mod.is_storage_active() and not svc_mod.is_nas_configured()

    def test_active_when_configured_and_enabled(self, monkeypatch, service):
        row = service.save(_input())
        self._patch_session(monkeypatch, row)
        assert svc_mod.get_active_storage_params().password == "hunter2"
        assert svc_mod.is_storage_active() and svc_mod.is_nas_configured()

    def test_storage_toggle_off_disables_writes_but_keeps_reads_working(self, monkeypatch, service):
        row = service.save(_input(use_as_storage=False))
        self._patch_session(monkeypatch, row)
        assert svc_mod.get_active_storage_params() is None
        assert not svc_mod.is_storage_active()
        # images already on the NAS must stay readable
        assert svc_mod.get_saved_params() is not None
