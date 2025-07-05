"""
Add unique constraints to catalog and engine names

Revision ID: f1620248ffed
Revises: 1c27711f42cd
Create Date: 2025-07-05 19:56:05.740247+00:00
"""

from alembic import op

revision = "f1620248ffed"
down_revision = "1c27711f42cd"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("catalog", schema=None) as batch_op:
        batch_op.create_unique_constraint(None, ["name"])
    with op.batch_alter_table("engine", schema=None) as batch_op:
        batch_op.create_unique_constraint(None, ["name"])


def downgrade():
    with op.batch_alter_table("engine", schema=None) as batch_op:
        batch_op.drop_constraint(None, type_="unique")
    with op.batch_alter_table("catalog", schema=None) as batch_op:
        batch_op.drop_constraint(None, type_="unique")
