"""Add query requests

Revision ID: de7ec1c82fe0
Revises: d61fb7e48cc3
Create Date: 2024-04-30 15:56:20.193978+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "de7ec1c82fe0"
down_revision = "d61fb7e48cc3"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "queryrequest",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column(
            "query_type",
            sa.Enum("METRICS", "MEASURES", "NODE", name="querybuildtype"),
            nullable=False,
        ),
        sa.Column(
            "nodes",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "parents",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "dimensions",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "filters",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("limit", sa.Integer(), nullable=True),
        sa.Column(
            "orderby",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("engine_name", sa.String(), nullable=True),
        sa.Column("engine_version", sa.String(), nullable=True),
        sa.Column(
            "other_args",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("query", sa.String(), nullable=False),
        sa.Column(
            "columns",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "query_type",
            "nodes",
            "parents",
            "dimensions",
            "filters",
            "engine_name",
            "engine_version",
            "limit",
            "orderby",
            name="query_request_unique",
            postgresql_nulls_not_distinct=True,
        ),
    )


def downgrade():
    op.drop_table("queryrequest")
    op.execute("DROP TYPE querybuildtype")
