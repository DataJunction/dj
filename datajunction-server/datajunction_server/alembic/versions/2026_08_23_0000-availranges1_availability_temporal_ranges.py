"""Store availability as a set of temporal ranges.

Revision ID: availranges1
Revises: nsboundaryflag1
Create Date: 2026-08-23 00:00:00.000000+00:00

"""

import json

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "availranges1"
down_revision = "nsboundaryflag1"
branch_labels = None
depends_on = None


def _range_type():
    """
    JSON on every dialect, JSONB on Postgres.
    """
    return sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def _as_list(value):
    """
    Read a stored bound, which may still be JSON text.
    """
    if isinstance(value, str):
        value = json.loads(value)
    return value or None


def upgrade():
    op.add_column(
        "availabilitystate",
        sa.Column("temporal_ranges", _range_type(), nullable=True),
    )
    connection = op.get_bind()
    rows = connection.execute(
        sa.text(
            "SELECT id, min_temporal_partition, max_temporal_partition "
            "FROM availabilitystate",
        ),
    ).fetchall()
    for row in rows:
        low, high = _as_list(row[1]), _as_list(row[2])
        ranges = (
            [{"min_temporal_partition": low, "max_temporal_partition": high}]
            if low or high
            else []
        )
        connection.execute(
            sa.text(
                "UPDATE availabilitystate SET temporal_ranges = :ranges WHERE id = :id",
            ).bindparams(sa.bindparam("ranges", type_=_range_type())),
            {"ranges": ranges, "id": row[0]},
        )
    op.drop_column("availabilitystate", "min_temporal_partition")
    op.drop_column("availabilitystate", "max_temporal_partition")


def downgrade():
    op.add_column(
        "availabilitystate",
        sa.Column("min_temporal_partition", sa.JSON(), nullable=True),
    )
    op.add_column(
        "availabilitystate",
        sa.Column("max_temporal_partition", sa.JSON(), nullable=True),
    )
    connection = op.get_bind()
    rows = connection.execute(
        sa.text("SELECT id, temporal_ranges FROM availabilitystate"),
    ).fetchall()
    for row in rows:
        ranges = _as_list(row[1]) or []
        low = ranges[0]["min_temporal_partition"] if ranges else None
        high = ranges[-1]["max_temporal_partition"] if ranges else None
        connection.execute(
            sa.text(
                "UPDATE availabilitystate SET min_temporal_partition = :low, "
                "max_temporal_partition = :high WHERE id = :id",
            ).bindparams(
                sa.bindparam("low", type_=sa.JSON()),
                sa.bindparam("high", type_=sa.JSON()),
            ),
            {"low": low, "high": high, "id": row[0]},
        )
    op.drop_column("availabilitystate", "temporal_ranges")
