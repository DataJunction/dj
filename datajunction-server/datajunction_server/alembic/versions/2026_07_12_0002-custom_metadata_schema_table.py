"""custom_metadata schema registry table

Revision ID: cm0002schematable
Revises: cm0001jsonbgin
Create Date: 2026-07-12 00:02:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "cm0002schematable"
down_revision = "nsboundaryflag1"
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
        sa.Column("owner", sa.String(), nullable=True),
        sa.Column("updated_by_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "reserved",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["created_by_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["updated_by_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_custommetadataschema_key_scope",
        "custommetadataschema",
        [
            sa.text("key"),
            sa.text("coalesce(node_type, '')"),
            sa.text("coalesce(namespace, '')"),
        ],
        unique=True,
        postgresql_where=sa.text("deactivated_at IS NULL"),
    )


def downgrade():
    op.drop_index(
        "uq_custommetadataschema_key_scope",
        table_name="custommetadataschema",
    )
    op.drop_table("custommetadataschema")
