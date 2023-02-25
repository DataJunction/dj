"""Add tags

Revision ID: 5b69cf676d4d
Revises: f3cbcd8dfac2
Create Date: 2023-02-24 18:31:24.395661+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "5b69cf676d4d"
down_revision = "f3cbcd8dfac2"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "tag",
        sa.Column("tag_metadata", sa.JSON(), nullable=True),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("display_name", sa.String(), nullable=True),
        sa.Column("description", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("tag_type", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_tag")),
        sa.UniqueConstraint("name", name=op.f("uq_tag_name")),
    )
    op.create_table(
        "tagnoderelationship",
        sa.Column("tag_id", sa.Integer(), nullable=False),
        sa.Column("node_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["node_id"],
            ["node.id"],
            name=op.f("fk_tagnoderelationship_node_id_node"),
        ),
        sa.ForeignKeyConstraint(
            ["tag_id"],
            ["tag.id"],
            name=op.f("fk_tagnoderelationship_tag_id_tag"),
        ),
        sa.PrimaryKeyConstraint(
            "tag_id",
            "node_id",
            name=op.f("pk_tagnoderelationship"),
        ),
    )


def downgrade():
    op.drop_table("tagnoderelationship")
    op.drop_table("tag")
