"""
Drop nodecolumns table

Revision ID: be76e22dd71a
Revises: 2282ef218abf
Create Date: 2025-09-26 17:40:27.501041+00:00
"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "be76e22dd71a"
down_revision = "2282ef218abf"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("nodecolumns", schema=None) as batch_op:
        batch_op.drop_index("idx_nodecolumns_node_id")

    op.drop_table("nodecolumns")


def downgrade():
    op.create_table(
        "nodecolumns",
        sa.Column("node_id", sa.BIGINT(), autoincrement=False, nullable=False),
        sa.Column("column_id", sa.BIGINT(), autoincrement=False, nullable=False),
        sa.ForeignKeyConstraint(
            ["column_id"],
            ["column.id"],
            name="fk_nodecolumns_column_id_column",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["node_id"],
            ["noderevision.id"],
            name="fk_nodecolumns_node_id_noderevision",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("node_id", "column_id", name="pk_nodecolumns"),
    )
    with op.batch_alter_table("nodecolumns", schema=None) as batch_op:
        batch_op.create_index("idx_nodecolumns_node_id", ["node_id"], unique=False)
