"""Add dimension links

Revision ID: 945d44abcd32
Revises: 724445d2b29d
Create Date: 2024-01-08 20:34:59.505580+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "945d44abcd32"
down_revision = "724445d2b29d"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "dimensionlink",
        sa.Column(
            "id",
            sa.BigInteger(),
            nullable=False,
        ),
        sa.Column("role", sa.String(), nullable=True),
        sa.Column(
            "node_revision_id",
            sa.BigInteger(),
            nullable=False,
        ),
        sa.Column(
            "dimension_id",
            sa.BigInteger(),
            nullable=False,
        ),
        sa.Column("join_sql", sa.String(), nullable=False),
        sa.Column(
            "join_type",
            sa.Enum("LEFT", "RIGHT", "INNER", "FULL", "CROSS", name="jointype"),
            nullable=True,
        ),
        sa.Column(
            "join_cardinality",
            sa.Enum(
                "ONE_TO_ONE",
                "ONE_TO_MANY",
                "MANY_TO_ONE",
                "MANY_TO_MANY",
                name="joincardinality",
            ),
            nullable=False,
        ),
        sa.Column("materialization_conf", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(
            ["dimension_id"],
            ["node.id"],
            name=op.f("fk_dimensionlink_dimension_id_node"),
        ),
        sa.ForeignKeyConstraint(
            ["node_revision_id"],
            ["noderevision.id"],
            name=op.f("fk_dimensionlink_node_revision_id_noderevision"),
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade():
    op.drop_table("dimensionlink")
