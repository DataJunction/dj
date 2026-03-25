"""add cascade delete to backfill materialization fk

Revision ID: a1b2c3d4e5f6
Revises: 3f8c2b1d4e9a
Create Date: 2026-03-25 00:00:00.000000+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op

revision = "a1b2c3d4e5f6"
down_revision = "3f8c2b1d4e9a"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint(
        "fk_backfill_materialization_id_materialization",
        "backfill",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_backfill_materialization_id_materialization",
        "backfill",
        "materialization",
        ["materialization_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade():
    op.drop_constraint(
        "fk_backfill_materialization_id_materialization",
        "backfill",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_backfill_materialization_id_materialization",
        "backfill",
        "materialization",
        ["materialization_id"],
        ["id"],
    )
