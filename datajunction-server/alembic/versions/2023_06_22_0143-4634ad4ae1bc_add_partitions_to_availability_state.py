"""Add partitions to availability state

Revision ID: 4634ad4ae1bc
Revises: 807eacf892a8
Create Date: 2023-06-22 01:43:12.954547+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel
from sqlalchemy.dialects import sqlite

from alembic import op

# revision identifiers, used by Alembic.
revision = "4634ad4ae1bc"
down_revision = "807eacf892a8"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "availabilitystate",
        sa.Column("categorical_partitions", sa.JSON(), nullable=True),
    )
    op.add_column(
        "availabilitystate",
        sa.Column("temporal_partitions", sa.JSON(), nullable=True),
    )
    op.add_column(
        "availabilitystate",
        sa.Column("min_temporal_partition", sa.JSON(), nullable=True),
    )
    op.add_column(
        "availabilitystate",
        sa.Column("partitions", sa.JSON(), nullable=True),
    )
    op.add_column(
        "availabilitystate",
        sa.Column("max_temporal_partition", sa.JSON(), nullable=True),
    )
    op.drop_column("availabilitystate", "min_partition")
    op.drop_column("availabilitystate", "max_partition")


def downgrade():
    op.add_column(
        "availabilitystate",
        sa.Column("max_partition", sqlite.JSON(), nullable=True),
    )
    op.add_column(
        "availabilitystate",
        sa.Column("min_partition", sqlite.JSON(), nullable=True),
    )
    op.drop_column("availabilitystate", "max_temporal_partition")
    op.drop_column("availabilitystate", "min_temporal_partition")
    op.drop_column("availabilitystate", "temporal_partitions")
    op.drop_column("availabilitystate", "categorical_partitions")
