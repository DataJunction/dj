"""add spark_hints to dimensionlink

Revision ID: a1b2c3d4e5f6
Revises: f7a8b9c0d1e2
Create Date: 2026-03-29 00:00:00.000000+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op

revision = "a1b2c3d4e5f6"
down_revision = "f7a8b9c0d1e2"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "dimensionlink",
        sa.Column(
            "spark_hints",
            sa.Enum(
                "broadcast",
                "merge",
                "shuffle_hash",
                "shuffle_replicate_nl",
                name="sparkjoinstrategy",
            ),
            nullable=True,
        ),
    )


def downgrade():
    op.drop_column("dimensionlink", "spark_hints")
    op.execute("DROP TYPE IF EXISTS sparkjoinstrategy")
