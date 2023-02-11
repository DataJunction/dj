"""Add attributes for cube nodes

Revision ID: 6bbaf7974a9d
Revises: b9f370ef912a
Create Date: 2023-02-10 19:23:44.130919+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "6bbaf7974a9d"
down_revision = "b9f370ef912a"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "cuberelationship",
        sa.Column("cube_id", sa.Integer(), nullable=False),
        sa.Column("cube_element_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["cube_element_id"],
            ["node.id"],
            name=op.f("fk_cuberelationship_cube_element_id_node"),
        ),
        sa.ForeignKeyConstraint(
            ["cube_id"],
            ["noderevision.id"],
            name=op.f("fk_cuberelationship_cube_id_noderevision"),
        ),
        sa.PrimaryKeyConstraint(
            "cube_id",
            "cube_element_id",
            name=op.f("pk_cuberelationship"),
        ),
    )


def downgrade():
    op.drop_table("cuberelationship")
