"""add EXTERNAL strategy value and name column to pre_aggregation

Revision ID: pa0002external
Revises: cm0001jsonbgin
Create Date: 2026-07-18 00:00:00.000000+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op

revision = "pa0002external"
down_revision = "cm0001jsonbgin"
branch_labels = None
depends_on = None


def upgrade():
    # Stable handle for externally-registered pre-aggs (YAML reconcile / callbacks).
    op.add_column(
        "pre_aggregation",
        sa.Column("name", sa.String(), nullable=True),
    )
    # Marks pre-aggs adopted from external tables (never DJ-materialized). The enum
    # values are stored as the enum member names (uppercase), matching the existing
    # materializationstrategy values.
    op.execute("ALTER TYPE materializationstrategy ADD VALUE IF NOT EXISTS 'EXTERNAL'")


def downgrade():
    op.drop_column("pre_aggregation", "name")
    # Postgres does not support removing a value from an enum type, so the
    # 'EXTERNAL' value is intentionally left in place on downgrade.
