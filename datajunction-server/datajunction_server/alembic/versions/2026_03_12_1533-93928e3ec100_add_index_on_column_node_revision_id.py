"""add index on column node_revision_id

Revision ID: 93928e3ec100
Revises: f6a7b8c9d0e1
Create Date: 2026-03-12 15:33:24.910204+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op

# revision identifiers, used by Alembic.
revision = '93928e3ec100'
down_revision = 'f6a7b8c9d0e1'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table('column', schema=None) as batch_op:
        batch_op.create_index(batch_op.f('ix_column_node_revision_id'), ['node_revision_id'], unique=False)


def downgrade():
    with op.batch_alter_table('column', schema=None) as batch_op:
        batch_op.drop_index(batch_op.f('ix_column_node_revision_id'))
