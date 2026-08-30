"""drop owner from custommetadataschema

Revision ID: cm0003dropowner
Revises: wf0001names
Create Date: 2026-08-30 00:00:00.000000+00:00

"""

import sqlalchemy as sa
from alembic import op

revision = "cm0003dropowner"
down_revision = "wf0001names"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column("custommetadataschema", "owner")


def downgrade():
    op.add_column(
        "custommetadataschema",
        sa.Column("owner", sa.String(), nullable=True),
    )
