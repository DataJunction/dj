"""
Add pre_aggregation table

Creates a first-class pre_aggregation table to store pre-aggregation entities
that can be shared across cubes. This replaces the embedded MeasuresMaterialization
JSON blob approach.

Key features:
- Grain group based on node_revision_id + grain (via grain_group_hash for lookup)
- Availability tracking via existing AvailabilityState model
- Independent materialization config per pre-agg (strategy, schedule, lookback_window)
- Reuses existing MaterializationStrategy enum (valid values: full, incremental_time)

Revision ID: 5a6b7c8d9e0f
Revises: 2a3b4c5d6e7f
Create Date: 2025-12-29 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "5a6b7c8d9e0f"
down_revision = "2a3b4c5d6e7f"
branch_labels = None
depends_on = None

# Reference to existing enum (don't create it)
materializationstrategy_enum = postgresql.ENUM(
    "FULL",
    "SNAPSHOT",
    "SNAPSHOT_PARTITION",
    "INCREMENTAL_TIME",
    "VIEW",
    name="materializationstrategy",
    create_type=False,
)


def upgrade():
    # Create pre_aggregation table
    # Note: Reuses existing 'materializationstrategy' enum type from materialization table
    op.create_table(
        "pre_aggregation",
        # Primary key
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
            autoincrement=True,
        ),
        # Identity fields - links to specific node revision
        sa.Column("node_revision_id", sa.BigInteger(), nullable=False),
        sa.Column("grain_columns", sa.JSON(), nullable=False),
        sa.Column("measures", sa.JSON(), nullable=False),
        # Output columns with types (grain columns + measure columns)
        # Nullable for backward compatibility
        sa.Column("columns", sa.JSON(), nullable=True),
        sa.Column("sql", sa.Text(), nullable=False),
        sa.Column("grain_group_hash", sa.String(), nullable=False),
        # Human-readable unique identifier: {node_name}-{hash}
        # Nullable for backward compatibility, unique and indexed for lookups
        sa.Column("slug", sa.String(), nullable=True, unique=True),
        # Materialization config - reuses existing MaterializationStrategy enum
        # Valid values for pre-aggs: FULL, INCREMENTAL_TIME
        sa.Column(
            "strategy",
            materializationstrategy_enum,
            nullable=True,
        ),
        sa.Column("schedule", sa.String(), nullable=True),
        sa.Column("lookback_window", sa.String(), nullable=True),
        # Workflow state
        sa.Column("scheduled_workflow_url", sa.String(), nullable=True),
        sa.Column("workflow_status", sa.String(), nullable=True),
        # Availability (links to existing availabilitystate table)
        sa.Column("availability_id", sa.BigInteger(), nullable=True),
        # Metadata
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        # Constraints
        sa.PrimaryKeyConstraint("id", name="pk_pre_aggregation"),
        sa.ForeignKeyConstraint(
            ["node_revision_id"],
            ["noderevision.id"],
            name="fk_pre_aggregation_node_revision_id_noderevision",
        ),
        sa.ForeignKeyConstraint(
            ["availability_id"],
            ["availabilitystate.id"],
            name="fk_pre_aggregation_availability_id_availabilitystate",
        ),
    )

    # Create index on grain_group_hash for fast lookups
    op.create_index(
        "ix_pre_aggregation_grain_group_hash",
        "pre_aggregation",
        ["grain_group_hash"],
    )

    # Create index on node_revision_id for finding pre-aggs by node revision
    op.create_index(
        "ix_pre_aggregation_node_revision_id",
        "pre_aggregation",
        ["node_revision_id"],
    )

    # Create index on slug for fast lookups (unique constraint already ensures uniqueness)
    op.create_index(
        "ix_pre_aggregation_slug",
        "pre_aggregation",
        ["slug"],
    )


def downgrade():
    # Drop indexes first
    op.drop_index("ix_pre_aggregation_slug", "pre_aggregation")
    op.drop_index("ix_pre_aggregation_node_revision_id", "pre_aggregation")
    op.drop_index("ix_pre_aggregation_grain_group_hash", "pre_aggregation")

    # Drop the table
    op.drop_table("pre_aggregation")
    # Note: Don't drop materializationstrategy enum - it's shared with materialization table
