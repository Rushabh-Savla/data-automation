import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Unit tests under tests/unit/ are pure-logic (no DB/Redis required) and must
# not fail just because CREDENTIAL_ENCRYPTION_KEY isn't set in the test env —
# only app.core.security.CredentialCipher() with no explicit key needs it,
# and the tests that exercise that pass their own key explicitly.
os.environ.setdefault("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/data_automation_test")
