"""Add column attributes

Revision ID: c4c787e2620f
Revises: 5b69cf676d4d
Create Date: 2023-03-09 16:00:47.224400+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "c4c787e2620f"
down_revision = "5b69cf676d4d"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "attributetype",
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("allowed_node_types", sa.JSON(), nullable=True),
        sa.Column("uniqueness_scope", sa.JSON(), nullable=True),
        sa.Column("namespace", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("description", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_attributetype")),
        sa.UniqueConstraint(
            "namespace",
            "name",
            name=op.f("uq_attributetype_namespace"),
        ),
    )
    op.create_table(
        "columnattribute",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("attribute_type_id", sa.Integer(), nullable=True),
        sa.Column("column_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["attribute_type_id"],
            ["attributetype.id"],
            name=op.f("fk_columnattribute_attribute_type_id_attributetype"),
        ),
        sa.ForeignKeyConstraint(
            ["column_id"],
            ["column.id"],
            name=op.f("fk_columnattribute_column_id_column"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_columnattribute")),
        sa.UniqueConstraint(
            "attribute_type_id",
            "column_id",
            name=op.f("uq_columnattribute_attribute_type_id"),
        ),
    )


def downgrade():
    op.drop_table("columnattribute")
    op.drop_table("attributetype")
