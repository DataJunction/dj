"""Add updated_at for availability state

Revision ID: af60d8073876
Revises: 84e6a136312d
Create Date: 2023-01-29 23:48:45.927434+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "af60d8073876"
down_revision = "84e6a136312d"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "availabilitystate",
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade():
    op.drop_column("availabilitystate", "updated_at")
