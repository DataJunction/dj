"""Add description and display_name to measures

Revision ID: b75e5163b09d
Revises: 879128f3e778
Create Date: 2023-09-21 00:33:52.761619+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "b75e5163b09d"
down_revision = "879128f3e778"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("measures", schema=None) as batch_op:
        batch_op.add_column(sa.Column("display_name", sa.String(), nullable=True))
        batch_op.add_column(
            sa.Column("description", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        )


def downgrade():
    with op.batch_alter_table("measures", schema=None) as batch_op:
        batch_op.drop_column("description")
        batch_op.drop_column("display_name")
