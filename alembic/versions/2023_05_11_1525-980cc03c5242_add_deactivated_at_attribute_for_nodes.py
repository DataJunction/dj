"""Add deactivated_at attribute for nodes

Revision ID: 980cc03c5242
Revises: e41c021c19a6
Create Date: 2023-05-11 15:25:36.936853+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "980cc03c5242"
down_revision = "e41c021c19a6"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "node",
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade():
    op.drop_column("node", "deactivated_at")
