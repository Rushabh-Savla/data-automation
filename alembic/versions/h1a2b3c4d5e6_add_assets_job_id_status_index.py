"""add assets job_id status index

Revision ID: h1a2b3c4d5e6
Revises: bfa1287ba6b1
Create Date: 2026-11-05 05:10:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'h1a2b3c4d5e6'
down_revision: Union[str, None] = 'bfa1287ba6b1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE INDEX IF NOT EXISTS ix_assets_job_id_status ON assets (job_id, status)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_assets_status ON assets (status)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_assets_is_duplicate_of ON assets (is_duplicate_of)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_candidate_reservations_asset_id ON candidate_reservations (asset_id)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_candidate_reservations_asset_id")
    op.execute("DROP INDEX IF EXISTS ix_assets_is_duplicate_of")
    op.execute("DROP INDEX IF EXISTS ix_assets_job_id_status")
    op.execute("DROP INDEX IF EXISTS ix_assets_status")
