"""
Add source metadata tables

Creates sourcetablemetadata and sourcepartitionmetadata tables to store
metadata about upstream source tables including size, row count, partition
ranges, and freshness information. This enables:
- Safe query execution with cost warnings
- Smart materialization defaults with cost estimates
- Data freshness monitoring
- Partition validation before incremental materialization

Populated by scheduled workflow that queries Iceberg __files metadata tables
and kragle/metacat.

Revision ID: e0f1g2h3i4j5
Revises: d4e5f6a7b8c9
Create Date: 2026-02-01 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e0f1g2h3i4j5"
down_revision = "d4e5f6a7b8c9"
branch_labels = None
depends_on = None


def upgrade():
    # Create sourcetablemetadata table
    op.create_table(
        "sourcetablemetadata",
        # Primary key
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
            autoincrement=True,
        ),
        # Foreign key to node
        sa.Column("node_id", sa.BigInteger(), nullable=False),
        # Table-level aggregates
        sa.Column("total_size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("total_row_count", sa.BigInteger(), nullable=False),
        sa.Column("total_partitions", sa.Integer(), nullable=False),
        # Partition range
        sa.Column("earliest_partition_value", sa.String(), nullable=True),
        sa.Column("latest_partition_value", sa.String(), nullable=True),
        # Freshness and retention
        sa.Column("freshness_timestamp", sa.BigInteger(), nullable=True),
        sa.Column("ttl_days", sa.Integer(), nullable=True),
        # Timestamps
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        # Constraints
        sa.PrimaryKeyConstraint("id", name="pk_sourcetablemetadata"),
        sa.ForeignKeyConstraint(
            ["node_id"],
            ["node.id"],
            name="fk_sourcetablemetadata_node_id_node",
            ondelete="CASCADE",
        ),
    )

    # Create index on node_id for fast lookups
    op.create_index(
        "idx_sourcetablemetadata_node_id",
        "sourcetablemetadata",
        ["node_id"],
    )

    # Create sourcepartitionmetadata table
    op.create_table(
        "sourcepartitionmetadata",
        # Primary key
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
            autoincrement=True,
        ),
        # Foreign key to source table metadata
        sa.Column(
            "source_table_metadata_id",
            sa.BigInteger(),
            nullable=False,
        ),
        # Partition value and statistics
        sa.Column("partition_value", sa.String(), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("row_count", sa.BigInteger(), nullable=False),
        # Timestamps
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        # Constraints
        sa.PrimaryKeyConstraint("id", name="pk_sourcepartitionmetadata"),
        sa.ForeignKeyConstraint(
            ["source_table_metadata_id"],
            ["sourcetablemetadata.id"],
            name="fk_sourcepartitionmetadata_source_table_metadata_id",
            ondelete="CASCADE",
        ),
    )

    # Create composite index on source_table_metadata_id and partition_value
    op.create_index(
        "idx_sourcepartitionmetadata_source_table_metadata_id_partition_value",
        "sourcepartitionmetadata",
        ["source_table_metadata_id", "partition_value"],
    )


def downgrade():
    # Drop indexes
    op.drop_index(
        "idx_sourcepartitionmetadata_source_table_metadata_id_partition_value",
        "sourcepartitionmetadata",
    )
    op.drop_index(
        "idx_sourcetablemetadata_node_id",
        "sourcetablemetadata",
    )

    # Drop tables (partition metadata first due to foreign key)
    op.drop_table("sourcepartitionmetadata")
    op.drop_table("sourcetablemetadata")
