"""Add status and mode attributes to Node

Revision ID: 20321580ea8a
Revises: f620c4521c80
Create Date: 2023-01-12 01:41:09.414015+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "20321580ea8a"
down_revision = "f620c4521c80"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "node",
        sa.Column("mode", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    )
    op.add_column(
        "node",
        sa.Column("status", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
    )


def downgrade():
    op.drop_column("node", "status")
    op.drop_column("node", "mode")
