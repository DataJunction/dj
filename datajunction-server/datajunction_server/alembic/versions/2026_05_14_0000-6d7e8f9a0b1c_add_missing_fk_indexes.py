"""add missing FK indexes for fast cascade deletes

Revision ID: 6d7e8f9a0b1c
Revises: 5a8b4c2d1e0f
Create Date: 2026-05-14 00:00:00.000000+00:00

Postgres does not auto-index the referencing side of a foreign key. When the
referenced row is deleted (with ON DELETE CASCADE or RESTRICT), Postgres must
locate matching child rows — without an index that's a sequential scan per
parent row. A 555-node hard-delete was spending ~55s here. Adding these
indexes turns those scans into fast index lookups.

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op

revision = "6d7e8f9a0b1c"
down_revision = "5a8b4c2d1e0f"
branch_labels = None
depends_on = None


# (table_name, index_name, [columns]) — one row per missing FK index.
FK_INDEXES = [
    # node.id referencers
    ("noderevision", "ix_noderevision_node_id", ["node_id"]),
    ("column", "ix_column_dimension_id", ["dimension_id"]),
    ("collectionnodes", "ix_collectionnodes_node_id", ["node_id"]),
    ("tagnoderelationship", "ix_tagnoderelationship_node_id", ["node_id"]),
    # noderevision.id referencers
    ("materialization", "ix_materialization_node_revision_id", ["node_revision_id"]),
    (
        "node_revision_frozen_measures",
        "ix_node_revision_frozen_measures_node_revision_id",
        ["node_revision_id"],
    ),
    ("pre_aggregation", "ix_pre_aggregation_node_revision_id", ["node_revision_id"]),
    ("nodeavailabilitystate", "ix_nodeavailabilitystate_node_id", ["node_id"]),
    (
        "metric_required_dimensions",
        "ix_metric_required_dimensions_metric_id",
        ["metric_id"],
    ),
    (
        "nodemissingparents",
        "ix_nodemissingparents_referencing_node_id",
        ["referencing_node_id"],
    ),
    (
        "frozen_measures",
        "ix_frozen_measures_upstream_revision_id",
        ["upstream_revision_id"],
    ),
    # column.id referencers
    ("partition", "ix_partition_column_id", ["column_id"]),
    ("columnattribute", "ix_columnattribute_column_id", ["column_id"]),
    ("cube", "ix_cube_cube_element_id", ["cube_element_id"]),
    (
        "metric_required_dimensions",
        "ix_metric_required_dimensions_bound_dimension_id",
        ["bound_dimension_id"],
    ),
    ("tablecolumns", "ix_tablecolumns_column_id", ["column_id"]),
    # extra node.id referencer
    (
        "hierarchy_levels",
        "ix_hierarchy_levels_dimension_node_id",
        ["dimension_node_id"],
    ),
    # frozen_measures.id referencers (huge win — 7.7s in cascade triggers)
    (
        "node_revision_frozen_measures",
        "ix_node_revision_frozen_measures_frozen_measure_id",
        ["frozen_measure_id"],
    ),
    # partition.id referencers
    ("column", "ix_column_partition_id", ["partition_id"]),
]


def upgrade():
    # Use raw SQL with IF NOT EXISTS so the migration is idempotent — some
    # of these indexes may have been created out-of-band on existing
    # environments.
    for table, name, cols in FK_INDEXES:
        cols_sql = ", ".join(f'"{c}"' for c in cols)
        op.execute(
            f'CREATE INDEX IF NOT EXISTS {name} ON "{table}" ({cols_sql})',
        )


def downgrade():
    for _table, name, _cols in reversed(FK_INDEXES):
        op.execute(f"DROP INDEX IF EXISTS {name}")
