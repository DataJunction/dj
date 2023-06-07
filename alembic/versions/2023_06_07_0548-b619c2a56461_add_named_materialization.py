"""Add named materialization

Revision ID: b619c2a56461
Revises: 2ff273e3b5de
Create Date: 2023-06-07 05:48:48.381107+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "b619c2a56461"
down_revision = "2ff273e3b5de"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "materializationconfig", sa.Column("name", sa.String(), nullable=True),
    )


def downgrade():
    op.drop_column("materializationconfig", "name")
