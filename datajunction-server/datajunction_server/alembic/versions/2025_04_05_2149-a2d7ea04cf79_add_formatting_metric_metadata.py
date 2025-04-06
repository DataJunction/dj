"""
Add formatting metric metadata

Revision ID: a2d7ea04cf79
Revises: 135fa5833fed
Create Date: 2025-04-05 21:49:34.079909+00:00
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a2d7ea04cf79"
down_revision = "135fa5833fed"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("metricmetadata", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "significant_digits",
                sa.Integer(),
                nullable=True,
                comment="Number of significant digits to display (if set).",
            ),
        )
        batch_op.add_column(
            sa.Column(
                "min_decimal_exponent",
                sa.Integer(),
                nullable=True,
                comment="Minimum exponent to still use decimal formatting; below this, use scientific notation.",
            ),
        )
        batch_op.add_column(
            sa.Column(
                "max_decimal_exponent",
                sa.Integer(),
                nullable=True,
                comment="Maximum exponent to still use decimal formatting; above this, use scientific notation.",
            ),
        )


def downgrade():
    with op.batch_alter_table("metricmetadata", schema=None) as batch_op:
        batch_op.drop_column("max_decimal_exponent")
        batch_op.drop_column("min_decimal_exponent")
        batch_op.drop_column("significant_digits")
