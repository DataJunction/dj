"""History table

Revision ID: 2ff273e3b5de
Revises: f8c3e437261b
Create Date: 2023-05-29 18:21:26.468042+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "2ff273e3b5de"
down_revision = "f8c3e437261b"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "history",
        sa.Column("pre", sa.JSON(), nullable=True),
        sa.Column("post", sa.JSON(), nullable=True),
        sa.Column("details", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("entity_type", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("entity_name", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("activity_type", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("user", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_history")),
    )


def downgrade():
    op.drop_table("history")
