"""
Add fixed_grain to node revisions

Revision ID: fg0001fixedgrain
Revises: rg0001reaggregate
Create Date: 2026-09-04 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "fg0001fixedgrain"
down_revision = "rg0001reaggregate"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "noderevision",
        sa.Column("fixed_grain", sa.JSON(), nullable=True),
    )


def downgrade():
    op.drop_column("noderevision", "fixed_grain")
