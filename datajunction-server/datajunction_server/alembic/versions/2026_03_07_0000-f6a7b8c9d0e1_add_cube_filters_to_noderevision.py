"""
Add cube_filters column to noderevision table

Stores cube-level filters as a JSON list of strings. These filters are always
applied when generating SQL for a cube node and are part of the cube's semantic
definition.

Revision ID: f6a7b8c9d0e1
Revises: e0f1g2h3i4j5
Create Date: 2026-03-07 00:00:00.000000+00:00
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "f6a7b8c9d0e1"
down_revision = "e0f1g2h3i4j5"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "noderevision",
        sa.Column("cube_filters", sa.JSON(), nullable=True),
    )


def downgrade():
    op.drop_column("noderevision", "cube_filters")
