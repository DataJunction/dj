"""Add categorical partitions to availability state

Revision ID: d1cc343da22a
Revises: a8f31238c03e
Create Date: 2023-06-21 23:02:05.263482+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op
from alembic.op import batch_alter_table

# revision identifiers, used by Alembic.
revision = "d1cc343da22a"
down_revision = "a8f31238c03e"
branch_labels = None
depends_on = None


def upgrade():
    with batch_alter_table("availabilitystate") as batch_op:
        batch_op.add_column(
            sa.Column("categorical_partitions", sa.JSON(), nullable=True, default=[]),
        )
        batch_op.add_column(sa.Column("temporal_partitions", sa.JSON(), nullable=True, default=[]))
        batch_op.add_column(
            sa.Column("min_temporal_partition", sa.JSON(), nullable=True, default=[]),
        )
        batch_op.add_column(
            sa.Column("max_temporal_partition", sa.JSON(), nullable=True, default=[]),
        )
        batch_op.drop_column("min_partition")
        batch_op.drop_column("max_partition")


def downgrade():
    with batch_alter_table("availabilitystate") as batch_op:
        batch_op.add_column(sa.Column("max_partition", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("min_partition", sa.JSON(), nullable=True))
        batch_op.drop_column("max_temporal_partition")
        batch_op.drop_column("min_temporal_partition")
        batch_op.drop_column("temporal_partitions")
        batch_op.drop_column("categorical_partitions")
