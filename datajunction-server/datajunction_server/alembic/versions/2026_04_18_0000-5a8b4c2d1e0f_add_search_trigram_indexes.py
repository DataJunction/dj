"""add GIN trigram indexes for search

Revision ID: 5a8b4c2d1e0f
Revises: 4f516e88e4d0
Create Date: 2026-04-18 00:00:00.000000+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op

revision = "5a8b4c2d1e0f"
down_revision = "4f516e88e4d0"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.create_index(
            "ix_noderevision_name_trgm",
            ["name"],
            unique=False,
            postgresql_using="gin",
            postgresql_ops={"name": "gin_trgm_ops"},
        )
        batch_op.create_index(
            "ix_noderevision_description_trgm",
            ["description"],
            unique=False,
            postgresql_using="gin",
            postgresql_ops={"description": "gin_trgm_ops"},
        )
    with op.batch_alter_table("tag", schema=None) as batch_op:
        batch_op.create_index(
            "ix_tag_name_trgm",
            ["name"],
            unique=False,
            postgresql_using="gin",
            postgresql_ops={"name": "gin_trgm_ops"},
        )
        batch_op.create_index(
            "ix_tag_description_trgm",
            ["description"],
            unique=False,
            postgresql_using="gin",
            postgresql_ops={"description": "gin_trgm_ops"},
        )


def downgrade():
    with op.batch_alter_table("tag", schema=None) as batch_op:
        batch_op.drop_index(
            "ix_tag_description_trgm",
            postgresql_using="gin",
            postgresql_ops={"description": "gin_trgm_ops"},
        )
        batch_op.drop_index(
            "ix_tag_name_trgm",
            postgresql_using="gin",
            postgresql_ops={"name": "gin_trgm_ops"},
        )
    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.drop_index(
            "ix_noderevision_description_trgm",
            postgresql_using="gin",
            postgresql_ops={"description": "gin_trgm_ops"},
        )
        batch_op.drop_index(
            "ix_noderevision_name_trgm",
            postgresql_using="gin",
            postgresql_ops={"name": "gin_trgm_ops"},
        )
