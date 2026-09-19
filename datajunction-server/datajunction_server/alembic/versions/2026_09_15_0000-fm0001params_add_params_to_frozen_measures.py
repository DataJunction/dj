"""
Add params column to frozen_measures

Adds a JSON column to persist tuning parameters (e.g., accuracy) for sketch-backed measures.

Revision ID: fm0001params
Revises: rg0001reaggregate
Create Date: 2026-09-15 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "fm0001params"
down_revision = "rg0001reaggregate"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "frozen_measures",
        sa.Column("params", sa.JSON(), nullable=True),
    )


def downgrade():
    op.drop_column("frozen_measures", "params")
