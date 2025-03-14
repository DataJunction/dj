"""Associate availability and materialization

Revision ID: b8ef80efd70c
Revises: c3d5f327296c
Create Date: 2025-02-24 05:49:06.588675+00:00

"""

# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module
import json
import sqlalchemy as sa
from sqlalchemy.sql import table, column
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "b8ef80efd70c"
down_revision = "c3d5f327296c"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("availabilitystate", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "custom_metadata",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=True,
            ),
        )
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
        column("custom_metadata", postgresql.JSONB),
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
                .values(custom_metadata=metadata),
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

    # Restore `url` and `links` from `custom_metadata`
    availabilitystate = table(
        "availabilitystate",
        column("id", sa.BigInteger()),
        column("custom_metadata", postgresql.JSONB),
        column("url", sa.String()),
        column("links", postgresql.JSON),
    )

    conn = op.get_bind()
    results = conn.execute(
        sa.select(availabilitystate.c.id, availabilitystate.c.custom_metadata),
    ).fetchall()

    for row in results:
        metadata = json.loads(row.custom_metadata) if row.custom_metadata else {}
        conn.execute(
            sa.update(availabilitystate)
            .where(availabilitystate.c.id == row.id)
            .values(
                url=metadata.get("url"),
                links=metadata.get("links"),
            ),
        )

    with op.batch_alter_table("availabilitystate", schema=None) as batch_op:
        batch_op.drop_column("materialization_id")
        batch_op.drop_column("custom_metadata")
