"""add index on node type

Revision ID: 3c7a9f1b2e4d
Revises: 7e1b9c4a2d3f
Create Date: 2026-06-11 18:40:18.157912+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op


# revision identifiers, used by Alembic.
revision = "3c7a9f1b2e4d"
down_revision = "7e1b9c4a2d3f"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("node", schema=None) as batch_op:
        batch_op.create_index("type_index", ["type"], unique=False)


def downgrade():
    with op.batch_alter_table("node", schema=None) as batch_op:
        batch_op.drop_index("type_index")
