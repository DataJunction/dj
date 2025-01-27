"""Add a map of links to availability state.

Revision ID: 4d6ab789e456
Revises: f3c9b40deb6f
Create Date: 2024-10-24 00:15:17.022358+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "4d6ab789e456"
down_revision = "f3c9b40deb6f"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("availabilitystate", schema=None) as batch_op:
        batch_op.add_column(sa.Column("links", sa.JSON(), nullable=True))


def downgrade():
    with op.batch_alter_table("availabilitystate", schema=None) as batch_op:
        batch_op.drop_column("links")
