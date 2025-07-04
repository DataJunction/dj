"""
Add node owner

Revision ID: 1c27711f42cd
Revises: 395952b010b0
Create Date: 2025-06-28 16:06:33.834754+00:00
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1c27711f42cd"
down_revision = "395952b010b0"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "node_owners",
        sa.Column(
            "node_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column("ownership_type", sa.String(length=256), nullable=True),
        sa.ForeignKeyConstraint(
            ["node_id"],
            ["node.id"],
            name="fk_node_owners_node_id",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name="fk_node_owners_user_id",
        ),
        sa.PrimaryKeyConstraint("node_id", "user_id"),
    )
    with op.batch_alter_table("node_owners", schema=None) as batch_op:
        batch_op.create_index("idx_node_owners_node_id", ["node_id"], unique=False)
        batch_op.create_index("idx_node_owners_user_id", ["user_id"], unique=False)

    # Autopopulate node_owners from node.created_by_id
    op.execute("""
        INSERT INTO node_owners (node_id, user_id)
        SELECT id, created_by_id
        FROM node
        WHERE created_by_id IS NOT NULL
    """)


def downgrade():
    with op.batch_alter_table("node_owners", schema=None) as batch_op:
        batch_op.drop_index("idx_node_owners_user_id")
        batch_op.drop_index("idx_node_owners_node_id")

    op.drop_table("node_owners")
