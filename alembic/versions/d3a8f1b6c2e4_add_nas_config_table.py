"""Add the nas_config table.

Holds the single UI-managed NAS connection (see app/db/models/nas_config.py).
Purely additive: no existing table or column is touched, so downgrade is a
plain drop and jobs/assets created before or after are unaffected.

Revision ID: d3a8f1b6c2e4
Revises: 9fc0b99f0097
"""

import sqlalchemy as sa
from alembic import op

revision = "d3a8f1b6c2e4"
down_revision = "9fc0b99f0097"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "nas_config",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=False, nullable=False),
        sa.Column("protocol", sa.String(length=16), nullable=False),
        sa.Column("host", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("port", sa.Integer(), nullable=True),
        sa.Column("share", sa.String(length=512), nullable=False, server_default=""),
        sa.Column("base_path", sa.String(length=1024), nullable=False, server_default=""),
        sa.Column("storage_path", sa.String(length=1024), nullable=False, server_default="data_automation"),
        sa.Column("use_as_storage", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("auth_mode", sa.String(length=16), nullable=False, server_default="anonymous"),
        sa.Column("username", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("domain", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("encrypted_password", sa.LargeBinary(), nullable=True),
        sa.Column("use_tls", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("verify_tls", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("id = 1", name="singleton"),
    )


def downgrade() -> None:
    op.drop_table("nas_config")
