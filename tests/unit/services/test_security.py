from cryptography.fernet import Fernet

from app.core.security import CredentialCipher


class TestCredentialCipher:
    def test_encrypt_decrypt_round_trip(self):
        key = Fernet.generate_key().decode()
        cipher = CredentialCipher(key=key)

        plaintext = "sk-super-secret-api-key-1234567890"
        encrypted = cipher.encrypt(plaintext)

        assert encrypted != plaintext.encode()
        assert cipher.decrypt(encrypted) == plaintext

    def test_different_keys_cannot_decrypt_each_other(self):
        cipher_a = CredentialCipher(key=Fernet.generate_key().decode())
        cipher_b = CredentialCipher(key=Fernet.generate_key().decode())

        encrypted = cipher_a.encrypt("some-secret")
        try:
            cipher_b.decrypt(encrypted)
            assert False, "expected decryption with the wrong key to fail"
        except RuntimeError:
            pass
