"""tag type claim table

Revision ID: tt0001claims
Revises: cm0003dropowner
Create Date: 2026-09-05 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op

revision = "tt0001claims"
down_revision = "cm0003dropowner"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "tagtypeclaim",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("tag_type", sa.String(), nullable=False),
        sa.Column("namespace", sa.String(), nullable=False),
        sa.Column("created_by_id", sa.BigInteger(), nullable=True),
        sa.Column("updated_by_id", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["created_by_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["updated_by_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tag_type"),
    )


def downgrade():
    op.drop_table("tagtypeclaim")
