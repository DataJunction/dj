"""Add URL to availability state

Revision ID: d8fa1e6371a7
Revises: b75e5163b09d
Create Date: 2023-10-02 16:42:04.639619+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "d8fa1e6371a7"
down_revision = "b75e5163b09d"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("availabilitystate", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("url", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        )


def downgrade():
    with op.batch_alter_table("availabilitystate", schema=None) as batch_op:
        batch_op.drop_column("url")
