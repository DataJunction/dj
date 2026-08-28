"""
Add reaggregate column to noderevision

Revision ID: rg0001reaggregate
Revises: nsboundaryflag1
Create Date: 2026-08-24 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "rg0001reaggregate"
down_revision = "nsboundaryflag1"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "noderevision",
        sa.Column("reaggregate", sa.JSON(), nullable=True),
    )


def downgrade():
    op.drop_column("noderevision", "reaggregate")
