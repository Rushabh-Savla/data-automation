"""Add staging_cleanup_status and staging_cleaned_at to nas_transfer_batches

Revision ID: g1a2b3c4d5e6
Revises: f1a2b3c4d5e6
Create Date: 2026-11-02 16:42:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "g1a2b3c4d5e6"
down_revision = "f1a2b3c4d5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "nas_transfer_batches",
        sa.Column("staging_cleanup_status", sa.String(length=32), nullable=False, server_default="none"),
    )
    op.add_column(
        "nas_transfer_batches",
        sa.Column("staging_cleaned_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_nas_transfer_batches_staging_cleanup_status",
        "nas_transfer_batches",
        ["staging_cleanup_status"],
    )


def downgrade() -> None:
    op.drop_index("ix_nas_transfer_batches_staging_cleanup_status", table_name="nas_transfer_batches")
    op.drop_column("nas_transfer_batches", "staging_cleaned_at")
    op.drop_column("nas_transfer_batches", "staging_cleanup_status")

