"""Add worker_id and heartbeat_at to deployments

Revision ID: hb0001heartbeat
Revises: rf0001reqfilters
Create Date: 2026-10-29 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "hb0001heartbeat"
down_revision = "rf0001reqfilters"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "deployments",
        sa.Column("worker_id", sa.String(), nullable=True),
    )
    op.add_column(
        "deployments",
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade():
    op.drop_column("deployments", "heartbeat_at")
    op.drop_column("deployments", "worker_id")
