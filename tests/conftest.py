import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Unit tests under tests/unit/ are pure-logic (no DB/Redis required) and must
# not fail just because CREDENTIAL_ENCRYPTION_KEY isn't set in the test env —
# only app.core.security.CredentialCipher() with no explicit key needs it,
# and the tests that exercise that pass their own key explicitly.
os.environ.setdefault("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/data_automation_test")
os.environ.setdefault("LOCAL_STAGING_PATH", "/tmp/data_automation_staging")
os.environ.setdefault("NAS_ROOT", "/tmp/data_automation_nas")

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import sessionmaker
from app.db.base import Base


@pytest.fixture
def db_session():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def register_sqlite_functions(dbapi_connection, connection_record):
        import datetime
        dbapi_connection.create_function("now", 0, lambda: datetime.datetime.now(datetime.timezone.utc).isoformat())
        dbapi_connection.create_function("greatest", -1, lambda *args: max(args) if args else None)

    from sqlalchemy.dialects.sqlite import JSON
    from sqlalchemy.dialects.postgresql import JSONB
    for table in Base.metadata.tables.values():
        for col in table.columns:
            if isinstance(col.type, JSONB):
                col.type = JSON()
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
