"""Add default_values to dimensionlink

Revision ID: dvs0001values
Revises: hb0001heartbeat
Create Date: 2026-11-12 00:00:00.000000+00:00

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "dvs0001values"
down_revision = "hb0001heartbeat"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("dimensionlink", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "default_values",
                sa.JSON().with_variant(postgresql.JSONB(), "postgresql"),
                nullable=True,
            ),
        )


def downgrade():
    with op.batch_alter_table("dimensionlink", schema=None) as batch_op:
        batch_op.drop_column("default_values")
