"""Add missing parents table and node attribute

Revision ID: 7caaf4276ea2
Revises: 20321580ea8a
Create Date: 2023-01-14 04:05:31.283222+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "7caaf4276ea2"
down_revision = "20321580ea8a"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "missingparent",
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "nodemissingparents",
        sa.Column("missing_parent_id", sa.Integer(), nullable=False),
        sa.Column("referencing_node_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["missing_parent_id"],
            ["missingparent.id"],
            name="fk_nodemissingparents_missing_parent_id",
        ),
        sa.ForeignKeyConstraint(
            ["referencing_node_id"],
            ["node.id"],
            name="fk_nodemissingparents_referencing_node_id",
        ),
        sa.PrimaryKeyConstraint("missing_parent_id", "referencing_node_id"),
    )


def downgrade():
    op.drop_table("nodemissingparents")
    op.drop_table("missingparent")
