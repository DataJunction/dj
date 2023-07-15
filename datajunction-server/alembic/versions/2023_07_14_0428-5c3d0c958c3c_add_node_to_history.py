"""Add node to history

Revision ID: 5c3d0c958c3c
Revises: 4e1ff36c27c6
Create Date: 2023-07-14 04:28:11.334887+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "5c3d0c958c3c"
down_revision = "bd313a10e2a8"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "history",
        sa.Column("node", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
    )


def downgrade():
    op.drop_column("history", "node")
