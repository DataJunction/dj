"""Add tables for Catalog and Engine models

Revision ID: e6430f249401
Revises: 1780b415e2c5
Create Date: 2023-02-04 21:30:50.113075+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "e6430f249401"
down_revision = "1780b415e2c5"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "catalog",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "engine",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("version", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "catalogengines",
        sa.Column("catalog_id", sa.Integer(), nullable=False),
        sa.Column("engine_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["catalog_id"],
            ["catalog.id"],
        ),
        sa.ForeignKeyConstraint(
            ["engine_id"],
            ["engine.id"],
        ),
        sa.PrimaryKeyConstraint("catalog_id", "engine_id"),
    )


def downgrade():
    op.drop_table("catalogengines")
    op.drop_table("engine")
    op.drop_table("catalog")
