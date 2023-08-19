"""store column-level lineage

Revision ID: fe8d3dbe512a
Revises: cde75f986a62
Create Date: 2023-08-18 23:48:40.205492+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "fe8d3dbe512a"
down_revision = "cde75f986a62"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("noderevision", sa.Column("lineage", sa.JSON(), nullable=True))


def downgrade():
    op.drop_column("noderevision", "lineage")
