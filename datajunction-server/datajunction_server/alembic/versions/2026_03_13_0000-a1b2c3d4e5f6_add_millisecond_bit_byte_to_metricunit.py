"""add millisecond, bit, byte to metricunit enum

Revision ID: a1b2c3d4e5f6
Revises: 93928e3ec100
Create Date: 2026-03-13 00:00:00.000000+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op

revision = "a1b2c3d4e5f6"
down_revision = "93928e3ec100"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TYPE metricunit ADD VALUE IF NOT EXISTS 'MILLISECOND'")
    op.execute("ALTER TYPE metricunit ADD VALUE IF NOT EXISTS 'BIT'")
    op.execute("ALTER TYPE metricunit ADD VALUE IF NOT EXISTS 'BYTE'")


def downgrade():
    # Postgres does not support removing enum values directly.
    # To downgrade, recreate the enum without the new values and migrate the column.
    op.execute(
        """
        DELETE FROM metricmetadata WHERE unit IN ('MILLISECOND', 'BIT', 'BYTE')
        """,
    )
    op.execute(
        """
        ALTER TYPE metricunit RENAME TO metricunit_old
        """,
    )
    op.execute(
        """
        CREATE TYPE metricunit AS ENUM (
            'UNKNOWN', 'UNITLESS', 'PERCENTAGE', 'PROPORTION', 'DOLLAR',
            'SECOND', 'MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR'
        )
        """,
    )
    op.execute(
        """
        ALTER TABLE metricmetadata
            ALTER COLUMN unit TYPE metricunit USING unit::text::metricunit
        """,
    )
    op.execute("DROP TYPE metricunit_old")
