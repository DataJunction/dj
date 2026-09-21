"""metric_required_dimensions: store authored ref instead of a bound Column

Replaces the `metric_required_dimensions` join table (metric_id,
bound_dimension_id -> column.id) with a normalized shape that stores the
authored reference string verbatim (`ref`), plus an indexed FK to the
dimension node it points at (`dimension_id`, nullable). This is what lets a
role-qualified required dimension (e.g. "date.dateint[created_date]") round
trip exactly instead of having its role silently stripped.

The backfill can't recover a role that was already stripped at write time
(that's the bug being fixed); it reconstructs the best available `ref` for
each existing row: the bare column name if the bound column's node is a
direct parent of the metric, else the bound column's `node.column` full
path (with `dimension_id` set to that node).

Revision ID: rd0001refcol
Revises: tt0001claims
Create Date: 2026-09-21 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op

revision = "rd0001refcol"
down_revision = "tt0001claims"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "metric_required_dimensions_new",
        sa.Column("metric_id", sa.BigInteger(), nullable=False),
        sa.Column("ref", sa.String(), nullable=False),
        sa.Column("dimension_id", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(
            ["metric_id"],
            ["noderevision.id"],
            name="fk_metric_required_dimensions_metric_id_noderevision",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["dimension_id"],
            ["node.id"],
            name="fk_metric_required_dimensions_dimension_id_node",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("metric_id", "ref"),
    )
    # Suffixed to avoid colliding with the old table's still-live indexes;
    # renamed to their final names below once the old table is dropped.
    op.create_index(
        "ix_metric_required_dimensions_metric_id_new",
        "metric_required_dimensions_new",
        ["metric_id"],
    )
    op.create_index(
        "ix_metric_required_dimensions_dimension_id_new",
        "metric_required_dimensions_new",
        ["dimension_id"],
    )

    # Backfill: for each existing (metric_id, bound_dimension_id) row, compute
    # `ref` using the same logic Column.full_name() callers relied on --
    # bare column name if the bound column's owning node is a direct parent
    # of the metric, else the owning node's fully-qualified `node.column`
    # path (role information was already lost at write time, so it can't be
    # recovered here; that's the bug this migration exists to fix going
    # forward).
    op.execute(
        """
        INSERT INTO metric_required_dimensions_new (metric_id, ref, dimension_id)
        SELECT
            old.metric_id,
            CASE
                WHEN dp.parent_id IS NOT NULL THEN c.name
                ELSE owner_node.name || '.' || c.name
            END AS ref,
            CASE
                WHEN dp.parent_id IS NOT NULL THEN NULL
                ELSE owner_node.id
            END AS dimension_id
        FROM metric_required_dimensions AS old
        JOIN "column" AS c ON c.id = old.bound_dimension_id
        JOIN noderevision AS owner_rev ON owner_rev.id = c.node_revision_id
        JOIN node AS owner_node ON owner_node.id = owner_rev.node_id
        LEFT JOIN noderelationship AS dp
            ON dp.child_id = old.metric_id AND dp.parent_id = owner_rev.node_id
        """,
    )

    op.drop_table("metric_required_dimensions")
    op.rename_table("metric_required_dimensions_new", "metric_required_dimensions")
    op.execute(
        "ALTER INDEX ix_metric_required_dimensions_metric_id_new "
        "RENAME TO ix_metric_required_dimensions_metric_id",
    )
    op.execute(
        "ALTER INDEX ix_metric_required_dimensions_dimension_id_new "
        "RENAME TO ix_metric_required_dimensions_dimension_id",
    )
    op.execute(
        "ALTER TABLE metric_required_dimensions "
        "RENAME CONSTRAINT metric_required_dimensions_new_pkey "
        "TO pk_metric_required_dimensions",
    )


def downgrade():
    op.create_table(
        "metric_required_dimensions_old",
        sa.Column("metric_id", sa.BigInteger(), nullable=False),
        sa.Column("bound_dimension_id", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(
            ["metric_id"],
            ["noderevision.id"],
            name="fk_metric_required_dimensions_metric_id_noderevision",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["bound_dimension_id"],
            ["column.id"],
            name="fk_metric_required_dimensions_bound_dimension_id_column",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("metric_id", "bound_dimension_id"),
    )
    op.create_index(
        "ix_metric_required_dimensions_metric_id_old",
        "metric_required_dimensions_old",
        ["metric_id"],
    )
    op.create_index(
        "ix_metric_required_dimensions_bound_dimension_id_old",
        "metric_required_dimensions_old",
        ["bound_dimension_id"],
    )

    # Best-effort: only bare-column refs (dimension_id IS NULL) can be mapped
    # back to a Column unambiguously (the metric's own parent). Full-path
    # refs, including any role suffix, have no Column to bind to anymore
    # (that's the whole point of this migration) and are dropped on downgrade.
    op.execute(
        """
        INSERT INTO metric_required_dimensions_old (metric_id, bound_dimension_id)
        SELECT new.metric_id, c.id
        FROM metric_required_dimensions AS new
        JOIN noderevision AS metric_rev ON metric_rev.id = new.metric_id
        JOIN noderelationship AS rel ON rel.child_id = new.metric_id
        JOIN noderevision AS parent_rev
            ON parent_rev.node_id = rel.parent_id AND parent_rev.version = (
                SELECT n.current_version FROM node AS n WHERE n.id = rel.parent_id
            )
        JOIN "column" AS c
            ON c.node_revision_id = parent_rev.id AND c.name = new.ref
        WHERE new.dimension_id IS NULL
        """,
    )

    op.drop_table("metric_required_dimensions")
    op.rename_table("metric_required_dimensions_old", "metric_required_dimensions")
    op.execute(
        "ALTER INDEX ix_metric_required_dimensions_metric_id_old "
        "RENAME TO ix_metric_required_dimensions_metric_id",
    )
    op.execute(
        "ALTER INDEX ix_metric_required_dimensions_bound_dimension_id_old "
        "RENAME TO ix_metric_required_dimensions_bound_dimension_id",
    )
    op.execute(
        "ALTER TABLE metric_required_dimensions "
        "RENAME CONSTRAINT metric_required_dimensions_old_pkey "
        "TO pk_metric_required_dimensions",
    )
