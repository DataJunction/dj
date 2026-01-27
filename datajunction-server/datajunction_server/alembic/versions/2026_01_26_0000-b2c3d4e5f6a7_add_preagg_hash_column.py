"""
Add preagg_hash column to pre_aggregation table

Adds a unique preagg_hash column that incorporates node_revision_id, grain_columns,
AND measure_expr_hashes. This ensures unique table/workflow names for pre-aggregations
with the same grain but different measures.

Revision ID: b2c3d4e5f6a7
Revises: b2c3d4e5f6g7
Create Date: 2026-01-26 00:00:00.000000+00:00
"""

import hashlib
import json

import sqlalchemy as sa
from alembic import op
from sqlalchemy.orm import Session

# revision identifiers, used by Alembic.
revision = "b2c3d4e5f6a7"
down_revision = "b2c3d4e5f6g7"
branch_labels = None
depends_on = None


def compute_preagg_hash(
    node_revision_id: int,
    grain_columns: list,
    measures: list,
) -> str:
    """Compute unique hash for a pre-aggregation."""
    # Extract expr_hash from each measure
    measure_hashes = sorted(
        [m.get("expr_hash", "") for m in measures if m.get("expr_hash")],
    )
    content = (
        f"{node_revision_id}:"
        f"{json.dumps(sorted(grain_columns))}:"
        f"{json.dumps(measure_hashes)}"
    )
    return hashlib.md5(content.encode()).hexdigest()[:8]


def upgrade():
    # Add preagg_hash column (nullable initially for migration)
    op.add_column(
        "pre_aggregation",
        sa.Column("preagg_hash", sa.String(8), nullable=True),
    )

    # Populate preagg_hash for existing rows
    connection = op.get_bind()
    session = Session(bind=connection)

    # Get all existing pre-aggregations
    result = connection.execute(
        sa.text(
            "SELECT id, node_revision_id, grain_columns, measures FROM pre_aggregation",
        ),
    )
    rows = result.fetchall()

    for row in rows:
        preagg_id = row[0]
        node_revision_id = row[1]
        grain_columns = (
            row[2] if isinstance(row[2], list) else json.loads(row[2] or "[]")
        )
        measures = row[3] if isinstance(row[3], list) else json.loads(row[3] or "[]")

        preagg_hash = compute_preagg_hash(node_revision_id, grain_columns, measures)

        connection.execute(
            sa.text("UPDATE pre_aggregation SET preagg_hash = :hash WHERE id = :id"),
            {"hash": preagg_hash, "id": preagg_id},
        )

    session.commit()

    # Make column non-nullable and add unique constraint
    op.alter_column("pre_aggregation", "preagg_hash", nullable=False)
    op.create_unique_constraint(
        "uq_pre_aggregation_preagg_hash",
        "pre_aggregation",
        ["preagg_hash"],
    )


def downgrade():
    op.drop_constraint(
        "uq_pre_aggregation_preagg_hash",
        "pre_aggregation",
        type_="unique",
    )
    op.drop_column("pre_aggregation", "preagg_hash")
