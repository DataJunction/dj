"""
Add frozen measures for node revisions

Revision ID: 8eab64955a49
Revises: 634fdac051c3
Create Date: 2025-08-02 15:43:20.001874+00:00
"""

import sqlalchemy as sa
from alembic import op

from datajunction_server.database.measure import MeasureAggregationRuleType
from datajunction_server.utils import get_settings

settings = get_settings()

# revision identifiers, used by Alembic.
revision = "8eab64955a49"
down_revision = "634fdac051c3"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "frozen_measures",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column(
            "upstream_revision_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column("expression", sa.String(), nullable=False),
        sa.Column("aggregation", sa.String(), nullable=False),
        sa.Column("rule", MeasureAggregationRuleType, nullable=False),
        sa.ForeignKeyConstraint(
            ["upstream_revision_id"],
            ["noderevision.id"],
            name="fk_frozen_measure_upstream_revision_id_noderevision",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "node_revision_frozen_measures",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column(
            "node_revision_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column("frozen_measure_id", sa.BigInteger(), nullable=False),
        sa.ForeignKeyConstraint(
            ["frozen_measure_id"],
            ["frozen_measures.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["node_revision_id"],
            ["noderevision.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.add_column(sa.Column("derived_expression", sa.String(), nullable=True))


def downgrade():
    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.drop_column("derived_expression")
    op.drop_table("node_revision_frozen_measures")
    op.drop_table("frozen_measures")
