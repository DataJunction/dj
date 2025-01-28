"""Add custom metadata field to nodes.

Revision ID: bec3296d7537
Revises: 9650f9b728a2
Create Date: 2025-01-24 00:20:26.333974+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "bec3296d7537"
down_revision = "9650f9b728a2"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.add_column(sa.Column("custom_metadata", sa.JSON(), nullable=True))


def downgrade():
    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.drop_column("custom_metadata")
