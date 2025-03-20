"""Added column description

Revision ID: 135fa5833fed
Revises: ae9eba981a2d
Create Date: 2025-03-14 23:04:39.248333+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "135fa5833fed"
down_revision = "ae9eba981a2d"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("column", schema=None) as batch_op:
        batch_op.add_column(sa.Column("description", sa.String(), nullable=True))


def downgrade():
    with op.batch_alter_table("column", schema=None) as batch_op:
        batch_op.drop_column("description")
