"""Add dimension reachability table for pre-computed transitive closure

Revision ID: 3a4b5c6d7e8f
Revises: 2a3b4c5d6e7f
Create Date: 2025-12-05 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op

revision = "3a4b5c6d7e8f"
down_revision = "2a3b4c5d6e7f"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "dimension_reachability",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("source_dimension_node_id", sa.BigInteger(), nullable=False),
        sa.Column("target_dimension_node_id", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(
            ["source_dimension_node_id"],
            ["node.id"],
            name="fk_dimension_reachability_source_node_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["target_dimension_node_id"],
            ["node.id"],
            name="fk_dimension_reachability_target_node_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_dimension_reachability"),
        sa.UniqueConstraint(
            "source_dimension_node_id",
            "target_dimension_node_id",
            name="uq_dimension_reachability_source_target",
        ),
    )

    op.create_index(
        "idx_dimension_reachability_source",
        "dimension_reachability",
        ["source_dimension_node_id"],
    )
    op.create_index(
        "idx_dimension_reachability_target",
        "dimension_reachability",
        ["target_dimension_node_id"],
    )


def downgrade():
    op.drop_index("idx_dimension_reachability_target", "dimension_reachability")
    op.drop_index("idx_dimension_reachability_source", "dimension_reachability")
    op.drop_table("dimension_reachability")
