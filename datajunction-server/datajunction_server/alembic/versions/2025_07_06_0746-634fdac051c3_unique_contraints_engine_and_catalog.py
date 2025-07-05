"""
Add unique contraints on the engine name + version and catalog name

Revision ID: 634fdac051c3
Revises: 1c27711f42cd
Create Date: 2025-07-06 07:46:50.840221+00:00
"""

from alembic import op

revision = "634fdac051c3"
down_revision = "1c27711f42cd"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("catalog", schema=None) as batch_op:
        batch_op.create_unique_constraint("uq_catalog_name", ["name"])

    with op.batch_alter_table("engine", schema=None) as batch_op:
        batch_op.create_unique_constraint("uq_engine_name_version", ["name", "version"])


def downgrade():
    with op.batch_alter_table("engine", schema=None) as batch_op:
        batch_op.drop_constraint("uq_engine_name_version", type_="unique")

    with op.batch_alter_table("catalog", schema=None) as batch_op:
        batch_op.drop_constraint("uq_catalog_name", type_="unique")
