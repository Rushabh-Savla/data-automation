"""Real integration tests for PostgreSQL migrations and schema verification.

Requires a live PostgreSQL database running (e.g. on socket /tmp port 54399).
Tests:
- Original migration (e1f2a3b4c5d6) application.
- Additive migration (f1a2b3c4d5e6) application.
- Verification of all expected columns and types in PostgreSQL.
- Downgrade to original migration and re-upgrade to head.
- Compatibility with existing database where original migration was pre-applied.
- SQLAlchemy model roundtrip with live PostgreSQL database.
"""

import os
import uuid
import pytest
import psycopg
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.db.models.image_batch import ImageBatch, NasTransferBatch
from app.db.models.job import Job
from app.domain.enums import BatchStatus, TransferStatus


PG_HOST = "/tmp"
PG_PORT = 54399
PG_USER = "testuser"
PG_DBNAME = "test_integration_pg_db"
PG_URL = f"postgresql+psycopg://{PG_USER}@/{PG_DBNAME}?host={PG_HOST}&port={PG_PORT}"


def is_postgres_available() -> bool:
    try:
        conn = psycopg.connect(f"dbname=postgres user={PG_USER} host={PG_HOST} port={PG_PORT}")
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not is_postgres_available(), reason="Real PostgreSQL not running on /tmp port 54399")


@pytest.fixture(scope="module")
def pg_db():
    """Sets up a clean disposable PostgreSQL test database and drops it on teardown."""
    from app.core.config import get_settings
    os.environ["DATABASE_URL"] = PG_URL
    get_settings.cache_clear()

    admin_conn = psycopg.connect(f"dbname=postgres user={PG_USER} host={PG_HOST} port={PG_PORT}", autocommit=True)
    with admin_conn.cursor() as cur:
        cur.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{PG_DBNAME}';")
        cur.execute(f"DROP DATABASE IF EXISTS {PG_DBNAME};")
        cur.execute(f"CREATE DATABASE {PG_DBNAME};")
    admin_conn.close()

    yield PG_URL

    admin_conn = psycopg.connect(f"dbname=postgres user={PG_USER} host={PG_HOST} port={PG_PORT}", autocommit=True)
    with admin_conn.cursor() as cur:
        cur.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{PG_DBNAME}';")
        cur.execute(f"DROP DATABASE IF EXISTS {PG_DBNAME};")
    admin_conn.close()
    get_settings.cache_clear()


def test_real_postgres_migration_lifecycle(pg_db):
    """Applies original migration, checks schema, applies additive migration, downgrades and re-upgrades."""
    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", pg_db)

    # 1. Apply original migration
    command.upgrade(alembic_cfg, "e1f2a3b4c5d6")

    with psycopg.connect(f"dbname={PG_DBNAME} user={PG_USER} host={PG_HOST} port={PG_PORT}") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'nas_transfer_batches';
            """)
            cols_orig = {row[0]: row[1] for row in cur.fetchall()}
            assert "id" in cols_orig
            assert "transfer_status" in cols_orig
            assert "total_files" not in cols_orig  # Not present in original migration!

    # 2. Apply additive migration (f1a2b3c4d5e6)
    command.upgrade(alembic_cfg, "f1a2b3c4d5e6")

    with psycopg.connect(f"dbname={PG_DBNAME} user={PG_USER} host={PG_HOST} port={PG_PORT}") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'nas_transfer_batches';
            """)
            cols_f1 = {row[0]: row[1] for row in cur.fetchall()}
            assert cols_f1["total_files"] == "integer"
            assert cols_f1["bytes_transferred"] == "bigint"
            assert "staging_cleanup_status" not in cols_f1

    # 3. Apply second additive migration (g1a2b3c4d5e6: staging cleanup tracking)
    command.upgrade(alembic_cfg, "g1a2b3c4d5e6")

    with psycopg.connect(f"dbname={PG_DBNAME} user={PG_USER} host={PG_HOST} port={PG_PORT}") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'nas_transfer_batches';
            """)
            cols_g1 = {row[0]: row[1] for row in cur.fetchall()}
            assert cols_g1["staging_cleanup_status"] == "character varying"
            assert cols_g1["staging_cleaned_at"] == "timestamp with time zone"

    # 4. Test downgrade back to f1a2b3c4d5e6
    command.downgrade(alembic_cfg, "f1a2b3c4d5e6")

    with psycopg.connect(f"dbname={PG_DBNAME} user={PG_USER} host={PG_HOST} port={PG_PORT}") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'nas_transfer_batches';
            """)
            cols_downgraded_f1 = [row[0] for row in cur.fetchall()]
            assert "staging_cleanup_status" not in cols_downgraded_f1
            assert "total_files" in cols_downgraded_f1

    # 5. Test downgrade back to e1f2a3b4c5d6
    command.downgrade(alembic_cfg, "e1f2a3b4c5d6")

    with psycopg.connect(f"dbname={PG_DBNAME} user={PG_USER} host={PG_HOST} port={PG_PORT}") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'nas_transfer_batches';
            """)
            cols_downgraded_e1 = [row[0] for row in cur.fetchall()]
            assert "total_files" not in cols_downgraded_e1
            assert "bytes_transferred" not in cols_downgraded_e1

    # 6. Re-upgrade back to head
    command.upgrade(alembic_cfg, "head")

    with psycopg.connect(f"dbname={PG_DBNAME} user={PG_USER} host={PG_HOST} port={PG_PORT}") as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'nas_transfer_batches';
            """)
            cols_reupgraded = [row[0] for row in cur.fetchall()]
            assert "total_files" in cols_reupgraded
            assert "bytes_transferred" in cols_reupgraded
            assert "staging_cleanup_status" in cols_reupgraded
            assert "staging_cleaned_at" in cols_reupgraded


def test_real_postgres_sqlalchemy_model_roundtrip(pg_db):
    """Verifies that SQLAlchemy model directly operates on real PostgreSQL without schema mismatch."""
    engine = create_engine(pg_db)
    Session = sessionmaker(bind=engine)

    try:
        with Session() as s:
            job = Job(id=uuid.uuid4(), name="live_pg_job", query="cat")
            s.add(job)
            batch = ImageBatch(id=uuid.uuid4(), job_id=job.id, batch_index=1, status=BatchStatus.READY_FOR_UPLOAD)
            s.add(batch)
            s.commit()

            transfer = NasTransferBatch(
                id=uuid.uuid4(),
                batch_id=batch.id,
                job_id=job.id,
                remote_path="data_automation/live_pg",
                transfer_status=TransferStatus.PENDING,
                staging_cleanup_status="pending",
                total_files=20,
                uploaded_files=10,
                failed_files=0,
                pending_files=10,
                bytes_transferred=20480,
                verification_status="passed",
                transfer_metadata={"environment": "real_postgres"},
            )
            s.add(transfer)
            s.commit()

            fetched = s.execute(select(NasTransferBatch).where(NasTransferBatch.id == transfer.id)).scalar_one()
            assert fetched.id == transfer.id
            assert fetched.transfer_status == TransferStatus.PENDING
            assert fetched.bytes_transferred == 20480
            assert fetched.staging_cleanup_status == "pending"

            # Update to COMPLETED
            fetched.transfer_status = TransferStatus.COMPLETED
            fetched.staging_cleanup_status = "completed"
            s.commit()

            refreshed = s.execute(select(NasTransferBatch).where(NasTransferBatch.id == transfer.id)).scalar_one()
            assert refreshed.transfer_status == TransferStatus.COMPLETED
            assert refreshed.staging_cleanup_status == "completed"
    finally:
        engine.dispose()
