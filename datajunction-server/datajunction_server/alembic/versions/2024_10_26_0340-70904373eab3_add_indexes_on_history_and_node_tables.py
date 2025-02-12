"""Add indexes on history and node tables

Revision ID: 70904373eab3
Revises: 4d6ab789e456
Create Date: 2024-10-26 03:40:04.657089+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op

# revision identifiers, used by Alembic.
revision = "70904373eab3"
down_revision = "4d6ab789e456"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("history", schema=None) as batch_op:
        batch_op.create_index("ix_history_entity_name", ["entity_name"], unique=False)

    with op.batch_alter_table("history", schema=None) as batch_op:
        batch_op.create_index("ix_history_user", ["user"], unique=False)

    with op.batch_alter_table("node", schema=None) as batch_op:
        batch_op.create_index(
            "cursor_index",
            ["created_at", "id"],
            unique=False,
            postgresql_using="btree",
        )

    with op.batch_alter_table("node", schema=None) as batch_op:
        batch_op.create_index(
            "namespace_index",
            ["namespace"],
            unique=False,
            postgresql_using="btree",
            postgresql_ops={"identifier": "varchar_pattern_ops"},
        )


def downgrade():
    with op.batch_alter_table("node", schema=None) as batch_op:
        batch_op.drop_index("namespace_index", postgresql_using="text_pattern_ops")

    with op.batch_alter_table("node", schema=None) as batch_op:
        batch_op.drop_index("cursor_index", postgresql_using="btree")

    with op.batch_alter_table("history", schema=None) as batch_op:
        batch_op.drop_index("ix_history_user")

    with op.batch_alter_table("history", schema=None) as batch_op:
        batch_op.drop_index("ix_history_entity_name")
