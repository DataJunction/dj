"""
Add ON DELETE CASCADE to foreign keys referencing noderevision

Fixes foreign key constraint violations when deleting nodes that have
related records in pre_aggregation, nodeavailabilitystate, materialization,
or nodemissingparents tables.

Revision ID: a1b2c3d4e5f7
Revises: f6562450c2c7
Create Date: 2026-01-20 00:00:00.000000+00:00
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f7"
down_revision = "f6562450c2c7"
branch_labels = None
depends_on = None


def upgrade():
    # Fix pre_aggregation.node_revision_id FK
    op.drop_constraint(
        "fk_pre_aggregation_node_revision_id_noderevision",
        "pre_aggregation",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_pre_aggregation_node_revision_id_noderevision",
        "pre_aggregation",
        "noderevision",
        ["node_revision_id"],
        ["id"],
        ondelete="CASCADE",
    )

    # Fix nodeavailabilitystate.node_id FK
    op.drop_constraint(
        "fk_nodeavailabilitystate_node_id_noderevision",
        "nodeavailabilitystate",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_nodeavailabilitystate_node_id_noderevision",
        "nodeavailabilitystate",
        "noderevision",
        ["node_id"],
        ["id"],
        ondelete="CASCADE",
    )

    # Fix materialization.node_revision_id FK
    op.drop_constraint(
        "fk_materialization_node_revision_id_noderevision",
        "materialization",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_materialization_node_revision_id_noderevision",
        "materialization",
        "noderevision",
        ["node_revision_id"],
        ["id"],
        ondelete="CASCADE",
    )

    # Fix nodemissingparents.referencing_node_id FK
    op.drop_constraint(
        "fk_nodemissingparents_referencing_node_id_noderevision",
        "nodemissingparents",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_nodemissingparents_referencing_node_id_noderevision",
        "nodemissingparents",
        "noderevision",
        ["referencing_node_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade():
    # Remove CASCADE from pre_aggregation.node_revision_id FK
    op.drop_constraint(
        "fk_pre_aggregation_node_revision_id_noderevision",
        "pre_aggregation",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_pre_aggregation_node_revision_id_noderevision",
        "pre_aggregation",
        "noderevision",
        ["node_revision_id"],
        ["id"],
    )

    # Remove CASCADE from nodeavailabilitystate.node_id FK
    op.drop_constraint(
        "fk_nodeavailabilitystate_node_id_noderevision",
        "nodeavailabilitystate",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_nodeavailabilitystate_node_id_noderevision",
        "nodeavailabilitystate",
        "noderevision",
        ["node_id"],
        ["id"],
    )

    # Remove CASCADE from materialization.node_revision_id FK
    op.drop_constraint(
        "fk_materialization_node_revision_id_noderevision",
        "materialization",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_materialization_node_revision_id_noderevision",
        "materialization",
        "noderevision",
        ["node_revision_id"],
        ["id"],
    )

    # Remove CASCADE from nodemissingparents.referencing_node_id FK
    op.drop_constraint(
        "fk_nodemissingparents_referencing_node_id_noderevision",
        "nodemissingparents",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_nodemissingparents_referencing_node_id_noderevision",
        "nodemissingparents",
        "noderevision",
        ["referencing_node_id"],
        ["id"],
    )
