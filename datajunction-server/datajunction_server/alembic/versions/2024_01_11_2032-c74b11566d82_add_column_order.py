"""Add column order and sets on delete policy for column <-> dimension_id

Revision ID: c74b11566d82
Revises: 945d44abcd32
Create Date: 2024-01-11 20:32:44.086208+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "c74b11566d82"
down_revision = "945d44abcd32"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("column", schema=None) as batch_op:
        batch_op.add_column(sa.Column("order", sa.Integer(), nullable=True))
        batch_op.drop_constraint("fk_column_dimension_id_node", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_column_dimension_id_node",
            "node",
            ["dimension_id"],
            ["id"],
            ondelete="SET NULL",
        )

        batch_op.drop_constraint("fk_column_measure_id_measures", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_column_measure_id_measures",
            "measures",
            ["measure_id"],
            ["id"],
            ondelete="SET NULL",
        )

        batch_op.drop_constraint("fk_column_partition_id_partition", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_column_partition_id_partition",
            "partition",
            ["partition_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade():
    with op.batch_alter_table("column", schema=None) as batch_op:
        batch_op.drop_constraint("fk_column_partition_id_partition", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_column_partition_id_partition",
            "partition",
            ["partition_id"],
            ["id"],
        )

        batch_op.drop_constraint("fk_column_measure_id_measures", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_column_measure_id_measures",
            "measures",
            ["measure_id"],
            ["id"],
        )

        batch_op.drop_constraint("fk_column_dimension_id_node", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_column_dimension_id_node",
            "node",
            ["dimension_id"],
            ["id"],
        )
        batch_op.drop_column("order")
