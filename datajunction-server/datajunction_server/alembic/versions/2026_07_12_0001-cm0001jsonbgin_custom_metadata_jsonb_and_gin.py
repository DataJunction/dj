"""custom_metadata JSON->JSONB + global GIN

Revision ID: cm0001jsonbgin
Revises: 3c7a9f1b2e4d
Create Date: 2026-07-12 00:01:00.000000+00:00
"""

from alembic import op

revision = "cm0001jsonbgin"
down_revision = "3c7a9f1b2e4d"
branch_labels = None
depends_on = None


def upgrade():
    # 200k rows — a straight in-place cast under a brief lock is fine.
    op.execute(
        "ALTER TABLE noderevision "
        "ALTER COLUMN custom_metadata TYPE jsonb "
        "USING custom_metadata::jsonb",
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_noderevision_custom_metadata_gin "
        "ON noderevision USING gin (custom_metadata jsonb_path_ops)",
    )


def downgrade():
    op.execute("DROP INDEX IF EXISTS ix_noderevision_custom_metadata_gin")
    op.execute(
        "ALTER TABLE noderevision "
        "ALTER COLUMN custom_metadata TYPE json "
        "USING custom_metadata::json",
    )
