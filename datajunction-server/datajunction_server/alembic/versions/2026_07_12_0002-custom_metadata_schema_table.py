"""custom_metadata schema registry table

Revision ID: cm0002schematable
Revises: cm0001jsonbgin
Create Date: 2026-07-12 00:02:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "cm0002schematable"
down_revision = "cm0001jsonbgin"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "custommetadataschema",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("key", sa.String(), nullable=False),
        sa.Column("node_type", sa.String(), nullable=True),
        sa.Column("namespace", sa.String(), nullable=True),
        sa.Column(
            "json_schema",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("value_kind", sa.String(), nullable=True),
        sa.Column(
            "filterable",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("created_by_id", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["created_by_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_custommetadataschema_key_type_ns",
        "custommetadataschema",
        ["key", "node_type", "namespace"],
        unique=True,
    )


def downgrade():
    op.drop_index(
        "uq_custommetadataschema_key_type_ns",
        table_name="custommetadataschema",
    )
    op.drop_table("custommetadataschema")
