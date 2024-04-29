"""Add queryrequest

Revision ID: 9897bf620fcc
Revises: d61fb7e48cc3
Create Date: 2024-04-28 16:39:12.830895+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "9897bf620fcc"
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
        sa.Column("nodes", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("dimensions", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("filters", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("engine_name", sa.String(), nullable=True),
        sa.Column("engine_version", sa.String(), nullable=True),
        sa.Column("limit", sa.Integer(), nullable=True),
        sa.Column("orderby", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("query", sa.String(), nullable=False),
        sa.Column("columns", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("other_args", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    with op.batch_alter_table("queryrequest", schema=None) as batch_op:
        batch_op.create_index(
            "query_request_unique",
            [
                "query_type",
                "nodes",
                "dimensions",
                "filters",
                "engine_name",
                "engine_version",
                "limit",
                "orderby",
            ],
            unique=True,
        )


def downgrade():
    with op.batch_alter_table("queryrequest", schema=None) as batch_op:
        batch_op.drop_index("query_request_unique")

    op.drop_table("queryrequest")
    op.execute("DROP TYPE querybuildtype")
