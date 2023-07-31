"""Add deactivated to materialization

Revision ID: 4147da2ac841
Revises: 5c3d0c958c3c
Create Date: 2023-07-30 16:08:40.416585+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "4147da2ac841"
down_revision = "5c3d0c958c3c"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "materialization",
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade():
    op.drop_column("materialization", "deactivated_at")
