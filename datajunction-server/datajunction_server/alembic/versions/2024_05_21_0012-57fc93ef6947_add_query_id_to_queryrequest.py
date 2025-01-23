"""Add query_id to queryrequest

Revision ID: 57fc93ef6947
Revises: 9b1227ff17f4
Create Date: 2024-05-21 00:12:11.303914+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "57fc93ef6947"
down_revision = "9b1227ff17f4"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("queryrequest", schema=None) as batch_op:
        batch_op.add_column(sa.Column("query_id", sa.String(), nullable=True))


def downgrade():
    with op.batch_alter_table("queryrequest", schema=None) as batch_op:
        batch_op.drop_column("query_id")
