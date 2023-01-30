"""Add UUID column to database model

Revision ID: f620c4521c80
Revises: 71b291295ae1
Create Date: 2022-10-27 22:42:19.160413+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module


import sqlalchemy as sa
import sqlalchemy_utils
import sqlmodel
from sqlalchemy.sql import table

from alembic import op

# revision identifiers, used by Alembic.
revision = "f620c4521c80"
down_revision = "71b291295ae1"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("database") as bop:
        bop.add_column(
            sa.Column("uuid", sqlalchemy_utils.types.uuid.UUIDType(), nullable=True),
        )


def downgrade():
    with op.batch_alter_table("database") as bop:
        bop.drop_column("uuid")
