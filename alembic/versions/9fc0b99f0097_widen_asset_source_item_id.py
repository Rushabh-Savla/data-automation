"""widen asset source_item_id column

Roboflow's real (working) Universe search now surfaces actual project
filenames, which frequently exceed the previous VARCHAR(128) limit on
assets.source_item_id — Roboflow appends a `.rf.<32-hex-hash>` suffix to
every exported filename on top of already-descriptive original names, and
this connector's id scheme (rf_{workspace}__{project}__v{version}__{split}__
{stem}) concatenates that with the workspace/project slugs. The insert then
fails with StringDataRightTruncation, which (before the matching fix in
_run_item_download) crashed the whole per-source chord and permanently
stranded the job in DOWNLOADING. 512 comfortably covers real-world Roboflow
names; the connector also now defensively hashes the tail of anything that
would still overflow it.

Revision ID: 9fc0b99f0097
Revises: c7d1a4e92f10
Create Date: 2026-09-17 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '9fc0b99f0097'
down_revision: Union[str, None] = 'c7d1a4e92f10'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column('assets', 'source_item_id', type_=sa.String(512), existing_type=sa.String(128), existing_nullable=False)


def downgrade() -> None:
    op.alter_column('assets', 'source_item_id', type_=sa.String(128), existing_type=sa.String(512), existing_nullable=False)
