"""
Add table-level size metadata to availabilitystate

Adds total_size_bytes, total_row_count, total_partitions, and ttl_days
columns to the availabilitystate table to support scan cost estimation.

Revision ID: e0f1g2h3i4j5
Revises: e5f6a7b8c9d0
Create Date: 2026-02-20 00:00:00.000000+00:00
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "e0f1g2h3i4j5"
down_revision = "e5f6a7b8c9d0"
branch_labels = None
depends_on = None


def upgrade():
    # Add table-level size metadata columns to availabilitystate
    op.add_column(
        "availabilitystate",
        sa.Column("total_size_bytes", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "availabilitystate",
        sa.Column("total_row_count", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "availabilitystate",
        sa.Column("total_partitions", sa.Integer(), nullable=True),
    )
    op.add_column(
        "availabilitystate",
        sa.Column("ttl_days", sa.Integer(), nullable=True),
    )


def downgrade():
    # Remove table-level size metadata columns from availabilitystate
    op.drop_column("availabilitystate", "ttl_days")
    op.drop_column("availabilitystate", "total_partitions")
    op.drop_column("availabilitystate", "total_row_count")
    op.drop_column("availabilitystate", "total_size_bytes")
