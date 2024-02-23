"""Cascade deletes to dimension links

Revision ID: d61fb7e48cc3
Revises: a8e22109be24
Create Date: 2024-02-22 07:13:51.441347+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "d61fb7e48cc3"
down_revision = "a8e22109be24"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("column", schema=None) as batch_op:
        batch_op.drop_constraint("fk_column_measure_id_measures", type_="foreignkey")
        batch_op.drop_constraint("fk_column_partition_id_partition", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_column_partition_id_partition",
            "partition",
            ["partition_id"],
            ["id"],
            ondelete="SET NULL",
        )
        batch_op.create_foreign_key(
            "fk_column_measure_id_measures",
            "measures",
            ["measure_id"],
            ["id"],
            ondelete="SET NULL",
        )

    with op.batch_alter_table("dimensionlink", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_dimensionlink_dimension_id_node",
            type_="foreignkey",
        )
        batch_op.drop_constraint(
            "fk_dimensionlink_node_revision_id_noderevision",
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_dimensionlink_node_revision_id_noderevision",
            "noderevision",
            ["node_revision_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            "fk_dimensionlink_dimension_id_node",
            "node",
            ["dimension_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("partition", schema=None) as batch_op:
        batch_op.drop_constraint("fk_partition_column_id_column", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_partition_column_id_column",
            "column",
            ["column_id"],
            ["id"],
            ondelete="CASCADE",
        )


def downgrade():
    with op.batch_alter_table("partition", schema=None) as batch_op:
        batch_op.drop_constraint("fk_partition_column_id_column", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_partition_column_id_column",
            "column",
            ["column_id"],
            ["id"],
        )

    with op.batch_alter_table("dimensionlink", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_dimensionlink_dimension_id_node",
            type_="foreignkey",
        )
        batch_op.drop_constraint(
            "fk_dimensionlink_node_revision_id_noderevision",
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_dimensionlink_node_revision_id_noderevision",
            "noderevision",
            ["node_revision_id"],
            ["id"],
        )
        batch_op.create_foreign_key(
            "fk_dimensionlink_dimension_id_node",
            "node",
            ["dimension_id"],
            ["id"],
        )

    with op.batch_alter_table("column", schema=None) as batch_op:
        batch_op.drop_constraint("fk_column_measure_id_measures", type_="foreignkey")
        batch_op.drop_constraint("fk_column_partition_id_partition", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_column_partition_id_partition",
            "partition",
            ["partition_id"],
            ["id"],
        )
        batch_op.create_foreign_key(
            "fk_column_measure_id_measures",
            "measures",
            ["measure_id"],
            ["id"],
        )
