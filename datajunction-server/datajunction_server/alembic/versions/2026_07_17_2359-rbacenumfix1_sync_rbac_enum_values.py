"""Sync RBAC enum values with ORM serialization."""

from alembic import op

revision = "rbacenumfix1"
down_revision = "pa0002external"
branch_labels = None
depends_on = None


def upgrade():
    for value in ("HIERARCHY", "ROLE", "ROLE_ASSIGNMENT", "ROLE_SCOPE"):
        op.execute(f"ALTER TYPE entitytype ADD VALUE IF NOT EXISTS '{value}'")


def downgrade():
    pass
