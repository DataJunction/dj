"""Availability state's valid_through_ts should be bigint

Revision ID: a8e22109be24
Revises: 20f060b02772
Create Date: 2024-01-23 16:55:20.951715+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'a8e22109be24'
down_revision = '20f060b02772'
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("availabilitystate", schema=None) as batch_op:
        batch_op.alter_column("valid_through_ts", type_=sa.BigInteger())


def downgrade():
    with op.batch_alter_table('availabilitystate', schema=None) as batch_op:
        batch_op.alter_column("valid_through_ts", type_=sa.Integer())
