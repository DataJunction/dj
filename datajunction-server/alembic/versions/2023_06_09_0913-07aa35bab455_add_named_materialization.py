"""Add named materialization

Revision ID: 07aa35bab455
Revises: 2ff273e3b5de
Create Date: 2023-06-09 09:13:59.939861+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op
from alembic.op import batch_alter_table

# revision identifiers, used by Alembic.
revision = "07aa35bab455"
down_revision = "2ff273e3b5de"
branch_labels = None
depends_on = None


def upgrade():
    with batch_alter_table("materializationconfig") as batch_op:
        batch_op.drop_constraint("pk_materializationconfig")
        batch_op.add_column(
            sa.Column("name", sa.String(), nullable=False, default="default"),
        )
        batch_op.create_primary_key(
            "pk_materializationconfig",
            ["node_revision_id", "name", "engine_id"],
        )


def downgrade():
    with batch_alter_table("materializationconfig") as batch_op:
        batch_op.drop_column("name")
        batch_op.drop_constraint("pk_materializationconfig")
        batch_op.create_primary_key(
            "pk_materializationconfig",
            ["node_revision_id", "engine_id"],
        )
