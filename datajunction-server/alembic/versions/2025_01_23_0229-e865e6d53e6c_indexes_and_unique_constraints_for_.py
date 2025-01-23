"""Indexes and unique constraints for notifiction_preferences table

Revision ID: e865e6d53e6c
Revises: 0e428c1bb9bc
Create Date: 2025-01-23 02:29:37.074889+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "e865e6d53e6c"
down_revision = "0e428c1bb9bc"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("notification_preferences", schema=None) as batch_op:
        batch_op.create_index("ix_entity_name", ["entity_name"], unique=False)
        batch_op.create_index("ix_entity_type", ["entity_type"], unique=False)
        batch_op.create_unique_constraint(
            "uix_entity_type_name",
            ["entity_type", "entity_name"],
        )


def downgrade():
    with op.batch_alter_table("notification_preferences", schema=None) as batch_op:
        batch_op.drop_constraint("uix_entity_type_name", type_="unique")
        batch_op.drop_index("ix_entity_type")
        batch_op.drop_index("ix_entity_name")
