"""Add default_value to dimensionlink

Revision ID: b2c3d4e5f6g7
Revises: a1b2c3d4e5f7
Create Date: 2026-01-26 00:00:00.000000+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "b2c3d4e5f6g7"
down_revision = "a1b2c3d4e5f7"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("dimensionlink", schema=None) as batch_op:
        batch_op.add_column(sa.Column("default_value", sa.String(), nullable=True))


def downgrade():
    with op.batch_alter_table("dimensionlink", schema=None) as batch_op:
        batch_op.drop_column("default_value")
