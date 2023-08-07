"""Add query ast to noderevision

Revision ID: 789f91d2d69e
Revises: ccc77abcf899
Create Date: 2023-08-07 14:32:54.290688+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "789f91d2d69e"
down_revision = "ccc77abcf899"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("noderevision", sa.Column("query_ast", sa.JSON(), nullable=True))


def downgrade():
    op.drop_column("noderevision", "query_ast")
