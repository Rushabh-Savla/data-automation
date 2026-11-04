"""Add image_batches, nas_transfer_batches tables and asset batch linking.

Supports configurable batch-based acquisition, persistent fingerprint indexing,
durable local staging manifests, and centralized sequential NAS transfer batches.

Revision ID: e1f2a3b4c5d6
Revises: d3a8f1b6c2e4
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "e1f2a3b4c5d6"
down_revision = "d3a8f1b6c2e4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Create image_batches table
    op.create_table(
        "image_batches",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("batch_index", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("target_image_count", sa.Integer(), nullable=False, server_default="600"),
        sa.Column("accepted_image_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rejected_image_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duplicate_image_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("uncertain_image_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_image_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("local_staging_path", sa.Text(), nullable=True),
        sa.Column("manifest_path", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="discovering"),
        sa.Column("batch_metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("job_id", "batch_index", name="uq_image_batches_job_id_batch_index"),
    )
    op.create_index("ix_image_batches_job_id", "image_batches", ["job_id"])
    op.create_index("ix_image_batches_status", "image_batches", ["status"])

    # 2. Create nas_transfer_batches table
    op.create_table(
        "nas_transfer_batches",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("batch_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("image_batches.id", ondelete="CASCADE"), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False),
        sa.Column("remote_path", sa.Text(), nullable=False),
        sa.Column("transfer_status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("checksum", sa.String(length=64), nullable=True),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_index("ix_nas_transfer_batches_batch_id", "nas_transfer_batches", ["batch_id"])
    op.create_index("ix_nas_transfer_batches_job_id", "nas_transfer_batches", ["job_id"])
    op.create_index("ix_nas_transfer_batches_transfer_status", "nas_transfer_batches", ["transfer_status"])

    # 3. Add batch_id and fast fingerprint lookup indexes to assets table
    op.add_column("assets", sa.Column("batch_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("image_batches.id", ondelete="SET NULL"), nullable=True))
    op.create_index("ix_assets_batch_id", "assets", ["batch_id"])
    op.create_index("ix_assets_checksum_sha256", "assets", ["checksum_sha256"])
    op.create_index("ix_assets_phash", "assets", ["phash"])


def downgrade() -> None:
    op.drop_index("ix_assets_phash", table_name="assets")
    op.drop_index("ix_assets_checksum_sha256", table_name="assets")
    op.drop_index("ix_assets_batch_id", table_name="assets")
    op.drop_column("assets", "batch_id")

    op.drop_index("ix_nas_transfer_batches_transfer_status", table_name="nas_transfer_batches")
    op.drop_index("ix_nas_transfer_batches_job_id", table_name="nas_transfer_batches")
    op.drop_index("ix_nas_transfer_batches_batch_id", table_name="nas_transfer_batches")
    op.drop_table("nas_transfer_batches")

    op.drop_index("ix_image_batches_status", table_name="image_batches")
    op.drop_index("ix_image_batches_job_id", table_name="image_batches")
    op.drop_table("image_batches")

