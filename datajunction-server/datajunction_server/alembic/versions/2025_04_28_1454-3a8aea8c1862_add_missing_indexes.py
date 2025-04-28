"""
Add missing indexes

Revision ID: 3a8aea8c1862
Revises: 51547dcccb10
Create Date: 2025-04-28 14:54:08.471638+00:00
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = '3a8aea8c1862'
down_revision = '51547dcccb10'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("columnattribute", schema=None) as batch_op:
        batch_op.create_index(
            "idx_columnattribute_column_id",
            ["column_id"],
            unique=False,
        )

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

    with op.batch_alter_table("metric_required_dimensions", schema=None) as batch_op:
        batch_op.create_index(
            "idx_metric_required_dimensions_metric_id",
            ["metric_id"],
            unique=False,
        )

    with op.batch_alter_table("node", schema=None) as batch_op:
        batch_op.create_index(
            "idx_node_deactivated_at_null",
            ["deactivated_at"],
            unique=False,
            postgresql_where=sa.text("deactivated_at IS NULL"),
        )

    with op.batch_alter_table("nodeavailabilitystate", schema=None) as batch_op:
        batch_op.create_index(
            "idx_nodeavailabilitystate_node_id",
            ["node_id"],
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

    with op.batch_alter_table("partition", schema=None) as batch_op:
        batch_op.create_index(
            "idx_partition_column_id",
            ["column_id"],
            unique=False,
            postgresql_using="btree",
        )

    with op.batch_alter_table("tagnoderelationship", schema=None) as batch_op:
        batch_op.create_index(
            "idx_tagnoderelationship_node_id",
            ["node_id"],
            unique=False,
        )
        batch_op.create_index(
            "idx_tagnoderelationship_tag_id",
            ["tag_id"],
            unique=False,
        )


def downgrade():
    with op.batch_alter_table("tagnoderelationship", schema=None) as batch_op:
        batch_op.drop_index("idx_tagnoderelationship_tag_id")
        batch_op.drop_index("idx_tagnoderelationship_node_id")

    with op.batch_alter_table("partition", schema=None) as batch_op:
        batch_op.drop_index("idx_partition_column_id", postgresql_using="btree")

    with op.batch_alter_table("noderelationship", schema=None) as batch_op:
        batch_op.drop_index("idx_noderelationship_parent_id")
        batch_op.drop_index("idx_noderelationship_child_id")

    with op.batch_alter_table("nodecolumns", schema=None) as batch_op:
        batch_op.drop_index("idx_nodecolumns_node_id")

    with op.batch_alter_table("nodeavailabilitystate", schema=None) as batch_op:
        batch_op.drop_index("idx_nodeavailabilitystate_node_id")

    with op.batch_alter_table("node", schema=None) as batch_op:
        batch_op.drop_index(
            "idx_node_deactivated_at_null",
            postgresql_where=sa.text("deactivated_at IS NULL"),
        )

    with op.batch_alter_table("metric_required_dimensions", schema=None) as batch_op:
        batch_op.drop_index("idx_metric_required_dimensions_metric_id")

    with op.batch_alter_table("dimensionlink", schema=None) as batch_op:
        batch_op.drop_index("idx_dimensionlink_node_revision_id")
        batch_op.drop_index("idx_dimensionlink_dimension_id")

    with op.batch_alter_table("cube", schema=None) as batch_op:
        batch_op.drop_index("idx_cube_cube_id")

    with op.batch_alter_table("columnattribute", schema=None) as batch_op:
        batch_op.drop_index("idx_columnattribute_column_id")
