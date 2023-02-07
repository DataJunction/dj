"""Add display name to nodes

Revision ID: 2bdcec2d24a7
Revises: 03aaf20e6ab6
Create Date: 2023-02-07 17:23:09.239258+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "2bdcec2d24a7"
down_revision = "03aaf20e6ab6"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("node") as batch_op:
        batch_op.add_column(sa.Column("display_name", sa.String(), nullable=True))
        batch_op.drop_constraint("uniq_node_name", type_="unique")
        batch_op.create_unique_constraint(
            op.f("uq_node_display_name"),
            ["display_name"],
        )
        batch_op.create_unique_constraint(op.f("uq_node_name"), ["name"])
    with op.batch_alter_table("noderevision") as batch_op:
        batch_op.add_column(sa.Column("display_name", sa.String(), nullable=True))
        batch_op.create_foreign_key(
            "fk_noderevision_display_name_node",
            "node",
            ["display_name"],
            ["display_name"],
        )


def downgrade():
    with op.batch_alter_table("noderevision") as batch_op:
        batch_op.drop_column("display_name")

    with op.batch_alter_table("node") as batch_op:
        batch_op.drop_constraint(op.f("uq_node_name"), type_="unique")
        batch_op.drop_constraint(op.f("uq_node_display_name"), type_="unique")
        batch_op.create_unique_constraint("uniq_node_name", ["name"])
        batch_op.drop_column("display_name")
