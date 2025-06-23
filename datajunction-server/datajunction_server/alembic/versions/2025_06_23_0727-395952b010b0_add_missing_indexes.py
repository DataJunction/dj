"""
Add indexes to speed up recursive CTE queries

Revision ID: 395952b010b0
Revises: 5a8eb7b2a9c6
Create Date: 2025-06-23 07:27:22.697566+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op

# revision identifiers, used by Alembic.
revision = "395952b010b0"
down_revision = "5a8eb7b2a9c6"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("cube", schema=None) as batch_op:
        batch_op.create_index("idx_cube_cube_id", ["cube_id"], unique=False)

    with op.batch_alter_table("dimensionlink", schema=None) as batch_op:
        batch_op.create_index(
            "idx_dimensionlink_dimension_id",
            ["dimension_id"],
            unique=False,
        )
        batch_op.create_index(
            "idx_dimensionlink_node_revision_id",
            ["node_revision_id"],
            unique=False,
        )

    with op.batch_alter_table("nodecolumns", schema=None) as batch_op:
        batch_op.create_index("idx_nodecolumns_node_id", ["node_id"], unique=False)

    with op.batch_alter_table("noderelationship", schema=None) as batch_op:
        batch_op.create_index(
            "idx_noderelationship_child_id",
            ["child_id"],
            unique=False,
        )
        batch_op.create_index(
            "idx_noderelationship_parent_id",
            ["parent_id"],
            unique=False,
        )


def downgrade():
    with op.batch_alter_table("noderelationship", schema=None) as batch_op:
        batch_op.drop_index("idx_noderelationship_parent_id")
        batch_op.drop_index("idx_noderelationship_child_id")

    with op.batch_alter_table("nodecolumns", schema=None) as batch_op:
        batch_op.drop_index("idx_nodecolumns_node_id")

    with op.batch_alter_table("dimensionlink", schema=None) as batch_op:
        batch_op.drop_index("idx_dimensionlink_node_revision_id")
        batch_op.drop_index("idx_dimensionlink_dimension_id")

    with op.batch_alter_table("cube", schema=None) as batch_op:
        batch_op.drop_index("idx_cube_cube_id")
