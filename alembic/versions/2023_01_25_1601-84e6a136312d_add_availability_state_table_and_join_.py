"""Add availability state table and join table

Revision ID: 84e6a136312d
Revises: 7caaf4276ea2
Create Date: 2023-01-25 16:01:38.166209+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "84e6a136312d"
down_revision = "7caaf4276ea2"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "availabilitystate",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("catalog", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("schema_", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("table", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("valid_through_ts", sa.Integer(), nullable=False),
        sa.Column("max_partition", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("min_partition", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "nodeavailabilitystate",
        sa.Column("availability_id", sa.Integer(), nullable=False),
        sa.Column("node_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["availability_id"],
            ["availabilitystate.id"],
            name="fk_nodeavailability_availability_id",
        ),
        sa.ForeignKeyConstraint(
            ["node_id"], ["node.id"], name="fk_nodeavailability_node_id",
        ),
        sa.PrimaryKeyConstraint("availability_id", "node_id"),
    )


def downgrade():
    op.drop_table("nodeavailabilitystate")
    op.drop_table("availabilitystate")
