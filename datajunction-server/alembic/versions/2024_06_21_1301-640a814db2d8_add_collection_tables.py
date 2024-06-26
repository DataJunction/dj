"""Add collection tables

Revision ID: 640a814db2d8
Revises: 57fc93ef6947
Create Date: 2024-06-21 13:01:48.141719+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "640a814db2d8"
down_revision = "57fc93ef6947"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "collection",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "collectionnodes",
        sa.Column("collection_id", sa.Integer(), nullable=False),
        sa.Column(
            "node_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["collection_id"],
            ["collection.id"],
            name="fk_collectionnodes_collection_id_collection",
        ),
        sa.ForeignKeyConstraint(
            ["node_id"],
            ["node.id"],
            name="fk_collectionnodes_node_id_node",
        ),
        sa.PrimaryKeyConstraint("collection_id", "node_id"),
    )


def downgrade():
    op.drop_table("collectionnodes")
    op.drop_table("collection")
