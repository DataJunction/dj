"""
Add cascade delete

Revision ID: 759c4d50cb8d
Revises: b6398ba852b3
Create Date: 2025-09-12 05:55:04.580692+00:00
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op

# revision identifiers, used by Alembic.
revision = "759c4d50cb8d"
down_revision = "5b00137c69f9"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("cube", schema=None) as batch_op:
        batch_op.drop_constraint("fk_cube_cube_element_id_column", type_="foreignkey")
        batch_op.drop_constraint("fk_cube_cube_id_noderevision", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_cube_cube_element_id_column",
            "column",
            ["cube_element_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            "fk_cube_cube_id_noderevision",
            "noderevision",
            ["cube_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("metric_required_dimensions", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_metric_required_dimensions_metric_id_noderevision",
            type_="foreignkey",
        )
        batch_op.drop_constraint(
            "fk_metric_required_dimensions_bound_dimension_id_column",
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_metric_required_dimensions_bound_dimension_id_column",
            "column",
            ["bound_dimension_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            "fk_metric_required_dimensions_metric_id_noderevision",
            "noderevision",
            ["metric_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("node_owners", schema=None) as batch_op:
        batch_op.drop_constraint("fk_node_owners_node_id", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_node_owners_node_id",
            "node",
            ["node_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("noderelationship", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_noderelationship_parent_id_node",
            type_="foreignkey",
        )
        batch_op.drop_constraint(
            "fk_noderelationship_child_id_noderevision",
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_noderelationship_parent_id_node",
            "node",
            ["parent_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            "fk_noderelationship_child_id_noderevision",
            "noderevision",
            ["child_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.drop_constraint("fk_noderevision_node_id_node", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_noderevision_node_id_node",
            "node",
            ["node_id"],
            ["id"],
            ondelete="CASCADE",
        )

    with op.batch_alter_table("nodecolumns", schema=None) as batch_op:
        batch_op.drop_constraint("fk_nodecolumns_column_id_column", type_="foreignkey")
        batch_op.drop_constraint(
            "fk_nodecolumns_node_id_noderevision",
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_nodecolumns_column_id_column",
            "column",
            ["column_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            "fk_nodecolumns_node_id_noderevision",
            "noderevision",
            ["node_id"],
            ["id"],
            ondelete="CASCADE",
        )


def downgrade():
    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.drop_constraint("fk_noderevision_node_id_node", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_noderevision_node_id_node",
            "node",
            ["node_id"],
            ["id"],
        )

    with op.batch_alter_table("noderelationship", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_noderelationship_child_id_noderevision",
            type_="foreignkey",
        )
        batch_op.drop_constraint(
            "fk_noderelationship_parent_id_node",
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_noderelationship_child_id_noderevision",
            "noderevision",
            ["child_id"],
            ["id"],
        )
        batch_op.create_foreign_key(
            "fk_noderelationship_parent_id_node",
            "node",
            ["parent_id"],
            ["id"],
        )

    with op.batch_alter_table("node_owners", schema=None) as batch_op:
        batch_op.drop_constraint("fk_node_owners_node_id", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_node_owners_node_id",
            "node",
            ["node_id"],
            ["id"],
        )

    with op.batch_alter_table("metric_required_dimensions", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_metric_required_dimensions_metric_id_noderevision",
            type_="foreignkey",
        )
        batch_op.drop_constraint(
            "fk_metric_required_dimensions_bound_dimension_id_column",
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_metric_required_dimensions_bound_dimension_id_column",
            "column",
            ["bound_dimension_id"],
            ["id"],
        )
        batch_op.create_foreign_key(
            "fk_metric_required_dimensions_metric_id_noderevision",
            "noderevision",
            ["metric_id"],
            ["id"],
        )

    with op.batch_alter_table("cube", schema=None) as batch_op:
        batch_op.drop_constraint("fk_cube_cube_id_noderevision", type_="foreignkey")
        batch_op.drop_constraint("fk_cube_cube_element_id_column", type_="foreignkey")
        batch_op.create_foreign_key(
            "fk_cube_cube_id_noderevision",
            "noderevision",
            ["cube_id"],
            ["id"],
        )
        batch_op.create_foreign_key(
            "fk_cube_cube_element_id_column",
            "column",
            ["cube_element_id"],
            ["id"],
        )

    with op.batch_alter_table("nodecolumns", schema=None) as batch_op:
        batch_op.drop_constraint("fk_nodecolumns_column_id_column", type_="foreignkey")
        batch_op.drop_constraint(
            "fk_nodecolumns_node_id_noderevision",
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_nodecolumns_column_id_column",
            "column",
            ["column_id"],
            ["id"],
        )
        batch_op.create_foreign_key(
            "fk_nodecolumns_node_id_noderevision",
            "noderevision",
            ["node_id"],
            ["id"],
        )
