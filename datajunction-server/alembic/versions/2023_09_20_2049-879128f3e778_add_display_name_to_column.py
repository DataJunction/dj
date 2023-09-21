"""Add display name to column

Revision ID: 879128f3e778
Revises: f2e9ef937daf
Create Date: 2023-09-20 20:49:28.773755+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "879128f3e778"
down_revision = "f2e9ef937daf"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("column", schema=None) as batch_op:
        batch_op.add_column(sa.Column("display_name", sa.String(), nullable=True))

    # Migrate existing columns by setting their display_name to a cleaned version of their name
    op.execute(
        'update "column" set display_name = '
        "(upper(substr(name, 0, 2)) || substr(replace(name, '_', ' '), 2))",
    )


def downgrade():
    with op.batch_alter_table("column", schema=None) as batch_op:
        batch_op.drop_column("display_name")
