"""
Switch to many-to-one relationship between columns and node revisions

Revision ID: 2282ef218abf
Revises: b6398ba852b3
Create Date: 2025-09-25 16:15:36.772156+00:00
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2282ef218abf"
down_revision = "b6398ba852b3"
branch_labels = None
depends_on = None


def upgrade():
    # Add the new column
    with op.batch_alter_table("column", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "node_revision_id",
                sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
                nullable=True,
            ),
        )
        batch_op.create_foreign_key(
            "fk_column_node_revision_id",
            "noderevision",
            ["node_revision_id"],
            ["id"],
            ondelete="CASCADE",
        )

    # Populate the column with the most recent NodeRevision per column
    op.execute("""
        UPDATE "column" c
        SET node_revision_id = sub.node_id
        FROM (
            SELECT DISTINCT ON (nc.column_id) nc.column_id,
                   nr.id AS node_id
            FROM nodecolumns nc
            JOIN noderevision nr ON nc.node_id = nr.id
            ORDER BY nc.column_id, nr.updated_at DESC
        ) sub
        WHERE c.id = sub.column_id;
    """)

    # Remove any orphaned columns that do not have a node_revision_id
    op.execute("""
        DELETE FROM "column" WHERE node_revision_id IS NULL;
    """)

    # Make it non-nullable
    with op.batch_alter_table("column") as batch_op:
        batch_op.alter_column("node_revision_id", nullable=False)


def downgrade():
    with op.batch_alter_table("column", schema=None) as batch_op:
        batch_op.drop_constraint("fk_column_node_revision_id", type_="foreignkey")
        batch_op.drop_column("node_revision_id")
