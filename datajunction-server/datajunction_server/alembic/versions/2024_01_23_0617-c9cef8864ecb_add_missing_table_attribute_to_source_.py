"""Add missing_table attribute to source nodes.

Revision ID: c9cef8864ecb
Revises: 20f060b02772
Create Date: 2024-01-23 06:17:10.054149+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "c9cef8864ecb"
down_revision = "20f060b02772"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("node", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("missing_table", sa.Boolean(), nullable=True, default=False),
        )

    op.execute("UPDATE node SET missing_table = False WHERE missing_table IS NULL")

    with op.batch_alter_table("node", schema=None) as batch_op:
        batch_op.alter_column("missing_table", nullable=False)


def downgrade():
    with op.batch_alter_table("node", schema=None) as batch_op:
        batch_op.drop_column("missing_table")
