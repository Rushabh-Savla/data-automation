"""Add recurring-schedule columns to jobs.

Scheduling was one-shot: a single scheduled_start_at that fired once and then
had no further meaning. A data-collection platform that is meant to replace a
person doing this work needs to keep collecting on a cadence, so a job can now
carry a cron expression and act as a template that spawns one child run per
firing.

Also backfills started_at for existing rows. JobService assigned job.status
directly and so bypassed the repository method that stamps it, leaving the
column NULL on every job ever started through the API -- including jobs that
were running at the time. created_at is the closest honest approximation
available retrospectively.

Revision ID: c7d1a4e92f10
Revises: b6a571f88c78
"""

import sqlalchemy as sa
from alembic import op

revision = "c7d1a4e92f10"
down_revision = "b6a571f88c78"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("schedule_cron", sa.String(length=128), nullable=True))
    op.add_column(
        "jobs",
        sa.Column("schedule_timezone", sa.String(length=64), nullable=False, server_default="UTC"),
    )
    op.add_column("jobs", sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("jobs", sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("jobs", sa.Column("parent_job_id", sa.dialects.postgresql.UUID(as_uuid=True), nullable=True))
    op.create_foreign_key(
        "fk_jobs_parent_job_id_jobs",
        "jobs",
        "jobs",
        ["parent_job_id"],
        ["id"],
        # SET NULL so deleting a template leaves its past runs (and their
        # collected assets) intact as standalone jobs.
        ondelete="SET NULL",
    )
    op.create_index("ix_jobs_status_next_run_at", "jobs", ["status", "next_run_at"])

    op.execute(
        """
        UPDATE jobs
        SET started_at = created_at
        WHERE started_at IS NULL AND status <> 'draft'
        """
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_status_next_run_at", table_name="jobs")
    op.drop_constraint("fk_jobs_parent_job_id_jobs", "jobs", type_="foreignkey")
    op.drop_column("jobs", "parent_job_id")
    op.drop_column("jobs", "last_run_at")
    op.drop_column("jobs", "next_run_at")
    op.drop_column("jobs", "schedule_timezone")
    op.drop_column("jobs", "schedule_cron")
