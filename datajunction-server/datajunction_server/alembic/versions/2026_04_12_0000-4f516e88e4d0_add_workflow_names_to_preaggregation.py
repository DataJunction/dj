"""add workflow_names to pre_aggregation

Revision ID: 4f516e88e4d0
Revises: c1d2e3f4a5b6
Create Date: 2026-04-12 00:00:00.000000+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op

revision = "4f516e88e4d0"
down_revision = "c1d2e3f4a5b6"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "pre_aggregation",
        sa.Column("workflow_names", sa.JSON(), nullable=True),
    )


def downgrade():
    op.drop_column("pre_aggregation", "workflow_names")
