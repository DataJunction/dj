"""Update backfill spec

Revision ID: 9b1227ff17f4
Revises: de7ec1c82fe0
Create Date: 2024-05-09 14:20:26.707322+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "9b1227ff17f4"
down_revision = "de7ec1c82fe0"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("backfill", schema=None) as batch_op:
        batch_op.alter_column(
            "spec",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            nullable=False,
        )

        # Update the backfill `spec` column to be a JSON list of partitions
        op.execute("UPDATE backfill SET spec = jsonb_build_array(spec)")


def downgrade():
    with op.batch_alter_table("backfill", schema=None) as batch_op:
        batch_op.alter_column(
            "spec",
            existing_type=postgresql.JSON(astext_type=sa.Text()),
            nullable=True,
        )

        # This will cause data loss
        op.execute("UPDATE backfill SET spec = spec->0")
