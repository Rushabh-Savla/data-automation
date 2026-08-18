"""fix frozen now() server_defaults

The initial migration passed server_default='now()' as a bare Python string
to several DateTime columns. SQLAlchemy/Postgres treats an unquoted-in-DDL
plain string default as a constant to be cast, not a live function call, so
Postgres evaluated it once at CREATE TABLE time and stored that single
timestamp as the column's permanent default — every row inserted without an
explicit value since then (i.e. every job, job_source_task, credential,
export_job, and asset in this database) got the exact same frozen
created_at/updated_at/requested_at/discovered_at, which is why job listings
(and anything else ordered by these columns) appeared unsorted by recency.
This does not, and cannot, repair already-inserted rows' frozen values —
only the column defaults, so new rows get a real per-insert timestamp
going forward.

Revision ID: 94c06c598b37
Revises: a4d5d5554584
Create Date: 2026-08-18 17:28:18.883359

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '94c06c598b37'
down_revision: Union[str, None] = 'a4d5d5554584'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_FIXES = [
    ("credentials", "created_at"),
    ("credentials", "updated_at"),
    ("jobs", "created_at"),
    ("jobs", "updated_at"),
    ("export_jobs", "requested_at"),
    ("job_source_tasks", "created_at"),
    ("assets", "discovered_at"),
]


def upgrade() -> None:
    for table, column in _FIXES:
        op.alter_column(table, column, server_default=sa.text("now()"))


def downgrade() -> None:
    # Restores the literal-string form for symmetry only — this reintroduces
    # the frozen-timestamp bug and should never actually be run.
    for table, column in _FIXES:
        op.alter_column(table, column, server_default="now()")
