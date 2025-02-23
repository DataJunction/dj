"""
Associate materialization with availability and change metadata

Revision ID: 0c3d8cd664b4
Revises: c3d5f327296c
Create Date: 2025-02-23 07:58:08.850294+00:00
"""

import json
import sqlalchemy as sa
from sqlalchemy.sql import table, column
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0c3d8cd664b4"
down_revision = "c3d5f327296c"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("availabilitystate", schema=None) as batch_op:
        batch_op.add_column(sa.Column("custom_metadata", sa.JSON(), nullable=True))
        batch_op.add_column(
            sa.Column("materialization_id", sa.BigInteger(), nullable=True),
        )
        batch_op.create_foreign_key(
            "fk_availability_materialization_id_materialization",
            "materialization",
            ["materialization_id"],
            ["id"],
        )

    availabilitystate = table(
        "availabilitystate",
        column("id", sa.BigInteger()),
        column("url", sa.String()),
        column("links", postgresql.JSON),
        column("custom_metadata", sa.JSON),
    )

    # Move data from url and links to custom_metadata
    connection = op.get_bind()
    results = connection.execute(
        sa.select(
            availabilitystate.c.id,
            availabilitystate.c.url,
            availabilitystate.c.links,
        ),
    ).fetchall()

    for row in results:
        metadata = {}
        if row.url:
            metadata["url"] = row.url
        if row.links:
            metadata["links"] = row.links

        if metadata:
            connection.execute(
                sa.update(availabilitystate)
                .where(availabilitystate.c.id == row.id)
                .values(custom_metadata=json.dumps(metadata)),
            )

    with op.batch_alter_table("availabilitystate", schema=None) as batch_op:
        batch_op.drop_column("url")
        batch_op.drop_column("links")


def downgrade():
    with op.batch_alter_table("availabilitystate", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "links",
                postgresql.JSON(astext_type=sa.Text()),
                autoincrement=False,
                nullable=True,
            ),
        )
        batch_op.add_column(
            sa.Column("url", sa.VARCHAR(), autoincrement=False, nullable=True),
        )
        batch_op.drop_constraint(
            "fk_availability_materialization_id_materialization",
            type_="foreignkey",
        )
        batch_op.drop_column("materialization_id")
        batch_op.drop_column("custom_metadata")

    # Restore `url` and `links` from `custom_metadata`
    availabilitystate = table(
        "availabilitystate",
        column("id", sa.BigInteger()),
        column("custom_metadata", sa.JSON),
        column("url", sa.String()),
        column("links", postgresql.JSON),
    )

    conn = op.get_bind()
    results = conn.execute(
        sa.select(availabilitystate.c.id, availabilitystate.c.custom_metadata),
    ).fetchall()

    for row in results:
        metadata = row.custom_metadata or {}
        conn.execute(
            sa.update(availabilitystate)
            .where(availabilitystate.c.id == row.id)
            .values(
                url=metadata.get("url"),
                links=metadata.get("links"),
            ),
        )
