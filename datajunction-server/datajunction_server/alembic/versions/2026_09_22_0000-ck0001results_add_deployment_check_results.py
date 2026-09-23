"""Add check_results to deployments

Revision ID: ck0001results
Revises: tt0001claims
Create Date: 2026-09-22 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "ck0001results"
down_revision = "tt0001claims"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "deployments",
        sa.Column("check_results", sa.JSON(), nullable=True),
    )


def downgrade():
    op.drop_column("deployments", "check_results")
