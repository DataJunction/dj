"""
Add index on display name

Revision ID: ae9eba981a2d
Revises: c3d5f327296c
Create Date: 2025-03-14 15:13:06.383265+00:00
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op

# revision identifiers, used by Alembic.
revision = "ae9eba981a2d"
down_revision = "c3d5f327296c"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.create_index(
            "ix_noderevision_display_name",
            ["display_name"],
            unique=False,
            postgresql_using="gin",
            postgresql_ops={"display_name": "gin_trgm_ops"},
        )


def downgrade():
    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.drop_index(
            "ix_noderevision_display_name",
            postgresql_using="gin",
            postgresql_ops={"display_name": "gin_trgm_ops"},
        )
    op.execute("DROP EXTENSION IF EXISTS pg_trgm;")
