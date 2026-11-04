"""
Add required_filters and recommended_filters to node revisions

Revision ID: rf0001reqfilters
Revises: rd0001refcol
Create Date: 2026-10-21 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "rf0001reqfilters"
down_revision = "rd0001refcol"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "noderevision",
        sa.Column("required_filters", sa.JSON(), nullable=True),
    )
    op.add_column(
        "noderevision",
        sa.Column("recommended_filters", sa.JSON(), nullable=True),
    )


def downgrade():
    op.drop_column("noderevision", "recommended_filters")
    op.drop_column("noderevision", "required_filters")
