"""
Add query ast for noderevision

Revision ID: 9650f9b728a2
Revises: 70904373eab3
Create Date: 2025-01-19 18:08:25.588956+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9650f9b728a2"
down_revision = "70904373eab3"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.add_column(sa.Column("query_ast", sa.PickleType(), nullable=True))


def downgrade():
    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.drop_column("query_ast")
