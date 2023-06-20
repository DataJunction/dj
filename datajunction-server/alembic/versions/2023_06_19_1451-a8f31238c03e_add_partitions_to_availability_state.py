"""Add partitions to availability state
Revision ID: a8f31238c03e
Revises: 807eacf892a8
Create Date: 2023-06-19 14:51:24.231461+00:00
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "a8f31238c03e"
down_revision = "807eacf892a8"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "availabilitystate",
        sa.Column("partitions", sa.JSON(), nullable=True),
    )


def downgrade():
    op.drop_column("availabilitystate", "partitions")
