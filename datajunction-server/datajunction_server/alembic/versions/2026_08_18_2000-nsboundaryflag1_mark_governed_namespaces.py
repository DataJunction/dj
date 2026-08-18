"""Mark governed namespace boundaries.

Revision ID: nsboundaryflag1
Revises: dw0001warnings
Create Date: 2026-08-18 20:00:00.000000+00:00

"""

import sqlalchemy as sa
from alembic import op

revision = "nsboundaryflag1"
down_revision = "dw0001warnings"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "nodenamespace",
        sa.Column(
            "is_governed_boundary",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
    )


def downgrade():
    op.drop_column("nodenamespace", "is_governed_boundary")
