"""add noderevision table index

Revision ID: tbl0001idx
Revises: fm0001params
Create Date: 2026-09-30 00:00:00.000000+00:00

"""

import sqlalchemy as sa
from alembic import op

revision = "tbl0001idx"
down_revision = "fm0001params"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.create_index(
            "ix_noderevision_table",
            [sa.text('lower("table")'), sa.text("lower(schema_)"), "catalog_id"],
            unique=False,
        )


def downgrade():
    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.drop_index("ix_noderevision_table")
