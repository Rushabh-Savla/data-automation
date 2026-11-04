"""Add detailed tracking columns to nas_transfer_batches table for centralized queue.

Revision ID: f1a2b3c4d5e6
Revises: e1f2a3b4c5d6
Create Date: 2026-11-02 16:15:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "f1a2b3c4d5e6"
down_revision: Union[str, None] = "e1f2a3b4c5d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Handle native enum if present in PostgreSQL
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_type WHERE typname = 'transferstatus') THEN
                ALTER TYPE transferstatus ADD VALUE IF NOT EXISTS 'completed';
            END IF;
        END $$;
        """
    )

    # Additive tracking columns to nas_transfer_batches
    op.add_column(
        "nas_transfer_batches",
        sa.Column("total_files", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "nas_transfer_batches",
        sa.Column("uploaded_files", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "nas_transfer_batches",
        sa.Column("failed_files", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "nas_transfer_batches",
        sa.Column("pending_files", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "nas_transfer_batches",
        sa.Column("bytes_transferred", sa.BigInteger(), nullable=False, server_default="0"),
    )
    op.add_column(
        "nas_transfer_batches",
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "nas_transfer_batches",
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "nas_transfer_batches",
        sa.Column("verification_status", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "nas_transfer_batches",
        sa.Column(
            "transfer_metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default="{}",
        ),
    )


def downgrade() -> None:
    op.drop_column("nas_transfer_batches", "transfer_metadata")
    op.drop_column("nas_transfer_batches", "verification_status")
    op.drop_column("nas_transfer_batches", "completed_at")
    op.drop_column("nas_transfer_batches", "started_at")
    op.drop_column("nas_transfer_batches", "bytes_transferred")
    op.drop_column("nas_transfer_batches", "pending_files")
    op.drop_column("nas_transfer_batches", "failed_files")
    op.drop_column("nas_transfer_batches", "uploaded_files")
    op.drop_column("nas_transfer_batches", "total_files")

