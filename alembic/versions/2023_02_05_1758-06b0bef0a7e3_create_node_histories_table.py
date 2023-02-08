"""Create node histories table

Revision ID: 06b0bef0a7e3
Revises: e6430f249401
Create Date: 2023-02-05 17:58:43.317077+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "06b0bef0a7e3"
down_revision = "e6430f249401"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "noderevision",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column(
            "type",
            sa.Enum("SOURCE", "TRANSFORM", "METRIC", "DIMENSION", name="nodetype"),
            nullable=True,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("description", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("query", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("mode", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("version", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("reference_node_id", sa.Integer(), nullable=True),
        sa.Column("status", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.ForeignKeyConstraint(
            ["reference_node_id"],
            ["node.id"],
            name="referencenode_reference_node_id_fk",
        ),
        sa.ForeignKeyConstraint(["name"], ["node.name"], name="referencenode_name_fk"),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "tablenoderevision",
        sa.Column("table_id", sa.Integer(), nullable=False),
        sa.Column("node_revision_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["node_revision_id"],
            ["noderevision.id"],
        ),
        sa.ForeignKeyConstraint(
            ["table_id"],
            ["table.id"],
        ),
        sa.PrimaryKeyConstraint("table_id", "node_revision_id"),
    )

    with op.batch_alter_table("node") as batch_op:
        batch_op.drop_column("mode")
        batch_op.drop_column("query")
        batch_op.drop_column("status")
        batch_op.drop_column("description")
        batch_op.drop_column("updated_at")

        batch_op.add_column(
            sa.Column(
                "current_version",
                sqlmodel.sql.sqltypes.AutoString(),
                nullable=False,
            ),
        )

    with op.batch_alter_table("nodeavailabilitystate") as batch_op:
        batch_op.drop_constraint("fk_nodeavailability_node_id", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_nodeavailability_node_id",
            "noderevision",
            ["node_id"],
            ["id"],
        )

    with op.batch_alter_table("nodecolumns") as batch_op:
        batch_op.drop_constraint("fk_nodecolumns_node_id", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_nodecolumns_node_id",
            "noderevision",
            ["node_id"],
            ["id"],
        )

    with op.batch_alter_table("nodemissingparents") as batch_op:
        batch_op.drop_constraint(
            "fk_nodemissingparents_referencing_node_id",
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_nodemissingparents_referencing_node_id",
            "noderevision",
            ["referencing_node_id"],
            ["id"],
        )

    with op.batch_alter_table("noderelationship") as batch_op:
        batch_op.add_column(
            sa.Column(
                "parent_version",
                sqlmodel.sql.sqltypes.AutoString(),
                nullable=True,
            ),
        )
        batch_op.drop_constraint("fk_noderelationship_child_id", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_noderelationship_child_id",
            "noderevision",
            ["child_id"],
            ["id"],
        )

    with op.batch_alter_table("table") as batch_op:
        batch_op.drop_constraint("fk_table_node_id", type_="foreignkey")
        batch_op.drop_column("node_id")


def downgrade():
    op.drop_constraint(None, "table", type_="foreignkey")
    op.create_foreign_key("fk_table_node_id", "table", "node", ["node_id"], ["id"])

    with op.batch_alter_table("noderelationship") as batch_op:
        batch_op.drop_constraint("fk_noderelationship_child_id", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_noderelationship_child_id",
            "node",
            ["child_id"],
            ["id"],
        )
        batch_op.drop_column("parent_version")

    with op.batch_alter_table("nodemissingparents") as batch_op:
        batch_op.drop_constraint(
            "fk_nodemissingparents_referencing_node_id",
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_nodemissingparents_referencing_node_id",
            "node",
            ["referencing_node_id"],
            ["id"],
        )

    with op.batch_alter_table("nodecolumns") as batch_op:
        batch_op.drop_constraint("fk_nodecolumns_node_id", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_nodecolumns_node_id",
            "node",
            ["node_id"],
            ["id"],
        )

    with op.batch_alter_table("nodeavailabilitystate") as batch_op:
        batch_op.drop_constraint("fk_nodeavailability_node_id", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_nodeavailability_node_id",
            "nodeavailabilitystate",
            "node",
            ["node_id"],
            ["id"],
        )

    with op.batch_alter_table("node") as batch_op:
        batch_op.add_column(sa.Column("updated_at", sa.TIMESTAMP(), nullable=True))
        batch_op.add_column(sa.Column("description", sa.VARCHAR(), nullable=False))
        batch_op.add_column(sa.Column("status", sa.VARCHAR(), nullable=False))
        batch_op.add_column(sa.Column("query", sa.VARCHAR(), nullable=True))
        batch_op.add_column(sa.Column("mode", sa.VARCHAR(), nullable=False))
        batch_op.drop_column("current_version")

    op.drop_table("noderevision")
    op.drop_table("tablenoderevision")
