"""Remove reference node usage

Revision ID: 03aaf20e6ab6
Revises: 06b0bef0a7e3
Create Date: 2023-02-07 16:40:22.905524+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "03aaf20e6ab6"
down_revision = "06b0bef0a7e3"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("noderevision") as batch_op:
        batch_op.add_column(sa.Column("node_id", sa.Integer(), nullable=True))
        batch_op.drop_constraint(
            "referencenode_reference_node_id_fk",
            type_="foreignkey",
        )
        batch_op.drop_constraint("referencenode_name_fk", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_noderevision_node_id_node",
            "node",
            ["node_id"],
            ["id"],
        )
        batch_op.drop_column("reference_node_id")


def downgrade():
    with op.batch_alter_table("noderevision") as batch_op:
        batch_op.add_column(sa.Column("reference_node_id", sa.INTEGER(), nullable=True))
        batch_op.drop_constraint(
            op.f("fk_noderevision_node_id_node"),
            type_="foreignkey",
        )
        batch_op.create_foreign_key("referencenode_name_fk", "node", ["name"], ["name"])
        batch_op.create_foreign_key(
            "referencenode_reference_node_id_fk",
            "node",
            ["reference_node_id"],
            ["id"],
        )
        batch_op.drop_column("noderevision", "node_id")
