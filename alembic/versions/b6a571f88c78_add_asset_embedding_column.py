"""add asset embedding column

Adds a nullable JSONB `embedding` column to `assets` — the CLIP image
embedding used as the additive "vector matching" signal for hybrid
duplicate detection (app/filtering/dedup.py), alongside the existing
`phash` column. Nullable because embedding computation can legitimately
fail/be skipped (CLIP load failure, corrupt image) and must degrade
gracefully, exactly like `phash` already does.

Revision ID: b6a571f88c78
Revises: 94c06c598b37
Create Date: 2026-08-20 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = 'b6a571f88c78'
down_revision: Union[str, None] = '94c06c598b37'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('assets', sa.Column('embedding', postgresql.JSONB(astext_type=sa.Text()), nullable=True))


def downgrade() -> None:
    op.drop_column('assets', 'embedding')
