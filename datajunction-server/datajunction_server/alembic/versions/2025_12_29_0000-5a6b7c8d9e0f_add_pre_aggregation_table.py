"""
Add pre_aggregation table

Creates a pre-aggregations table to store pre-aggregation entities that can be
shared across cubes. Includes workflow state for scheduler-agnostic workflow
URL tracking.

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

# Reference to existing enum
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
        sa.Column("node_revision_id", sa.BigInteger(), nullable=False),
        sa.Column("grain_columns", sa.JSON(), nullable=False),
        sa.Column("measures", sa.JSON(), nullable=False),
        sa.Column("columns", sa.JSON(), nullable=True),
        sa.Column("sql", sa.Text(), nullable=False),
        sa.Column("grain_group_hash", sa.String(), nullable=False),
        sa.Column(
            "strategy",
            materializationstrategy_enum,
            nullable=True,
        ),
        sa.Column("schedule", sa.String(), nullable=True),
        sa.Column("lookback_window", sa.String(), nullable=True),
        sa.Column("workflow_urls", sa.JSON(), nullable=True),
        sa.Column("workflow_status", sa.String(), nullable=True),
        sa.Column("availability_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
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


def downgrade():
    op.drop_index("ix_pre_aggregation_node_revision_id", "pre_aggregation")
    op.drop_index("ix_pre_aggregation_grain_group_hash", "pre_aggregation")
    op.drop_table("pre_aggregation")
