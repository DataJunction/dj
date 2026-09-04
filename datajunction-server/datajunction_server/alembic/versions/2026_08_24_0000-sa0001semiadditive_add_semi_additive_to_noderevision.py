"""
Add semi_additive column to noderevision

Revision ID: sa0001semiadditive
Revises: nsboundaryflag1
Create Date: 2026-08-24 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "sa0001semiadditive"
down_revision = "nsboundaryflag1"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "noderevision",
        sa.Column("semi_additive", sa.JSON(), nullable=True),
    )


def downgrade():
    op.drop_column("noderevision", "semi_additive")
