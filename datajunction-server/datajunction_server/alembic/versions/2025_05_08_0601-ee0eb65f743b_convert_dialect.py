"""
Convert dialect

Revision ID: ee0eb65f743b
Revises: 51547dcccb10
Create Date: 2025-05-08 06:01:38.039079+00:00
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "ee0eb65f743b"
down_revision = "51547dcccb10"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("engine", schema=None) as batch_op:
        batch_op.alter_column(
            "dialect",
            existing_type=postgresql.ENUM("SPARK", "TRINO", "DRUID", name="dialect"),
            type_=sa.String(),
            existing_nullable=True,
        )


def downgrade():
    with op.batch_alter_table("engine", schema=None) as batch_op:
        batch_op.alter_column(
            "dialect",
            existing_type=sa.String(),
            type_=postgresql.ENUM("SPARK", "TRINO", "DRUID", name="dialect"),
            existing_nullable=True,
        )
