"""add unit JSONB column to column table

Revision ID: 7e1b9c4a2d3f
Revises: 6d7e8f9a0b1c
Create Date: 2026-05-19 00:00:00.000000+00:00

Adds a structured `unit` column to `column` for column-level unit metadata.
Atomic shape: {"kind": "currency", "code": "USD"}.
Compound shape: {"numerator": {...}, "denominator": {...}}.

Validation lives at the Pydantic model layer
(datajunction_server.models.unit.Unit); the DB just stores JSONB.

This migration is purely additive — no existing rows are touched, no other
table or column changes. Subsequent migrations (lift unit from
metricmetadata.unit, backfill historical data) ship in separate PRs.
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "7e1b9c4a2d3f"
down_revision = "6d7e8f9a0b1c"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "column",
        sa.Column("unit", JSONB(), nullable=True),
    )


def downgrade():
    op.drop_column("column", "unit")
