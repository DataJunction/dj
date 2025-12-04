"""
Add last_viewed_notifications_at to users table

Revision ID: 2a3b4c5d6e7f
Revises: 1a2b3c4d5e6f
Create Date: 2025-12-03 00:00:00.000000+00:00
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2a3b4c5d6e7f"
down_revision = "1a2b3c4d5e6f"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "users",
        sa.Column(
            "last_viewed_notifications_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )


def downgrade():
    op.drop_column("users", "last_viewed_notifications_at")
