"""add workflow_names to materialization

Revision ID: wf0001names
Revises: cm0002schematable
Create Date: 2026-08-26 00:00:00.000000+00:00

"""

import sqlalchemy as sa
from alembic import op

revision = "wf0001names"
down_revision = "cm0002schematable"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "materialization",
        sa.Column("workflow_names", sa.JSON(), nullable=True),
    )


def downgrade():
    op.drop_column("materialization", "workflow_names")
