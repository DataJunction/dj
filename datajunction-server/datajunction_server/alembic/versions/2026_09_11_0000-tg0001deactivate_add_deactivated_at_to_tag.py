"""add deactivated_at to tag

Revision ID: tg0001deactivate
Revises: cm0003dropowner
Create Date: 2026-09-11 00:00:00.000000+00:00

"""

import sqlalchemy as sa
from alembic import op

revision = "tg0001deactivate"
down_revision = "cm0003dropowner"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "tag",
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade():
    op.drop_column("tag", "deactivated_at")
