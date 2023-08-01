"""Add deactivated_at flag to NodeNamespace.

Revision ID: c99be6d9a847
Revises: 5c3d0c958c3c
Create Date: 2023-07-31 18:20:06.393194+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "c99be6d9a847"
down_revision = "5c3d0c958c3c"
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "nodenamespace",
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("nodenamespace", "deactivated_at")
    # ### end Alembic commands ###