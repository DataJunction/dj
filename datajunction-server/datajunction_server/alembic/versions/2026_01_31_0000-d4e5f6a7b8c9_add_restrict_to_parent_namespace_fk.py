"""
Add RESTRICT to parent_namespace foreign key

Updates the parent_namespace FK constraint to explicitly use RESTRICT on delete,
preventing deletion of parent namespaces that have child branch namespaces.

This ensures that users must explicitly delete or reassign child namespaces
before deleting a parent, preventing accidental data loss.

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-01-31 00:00:00.000000+00:00
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "d4e5f6a7b8c9"
down_revision = "c3d4e5f6a7b8"
branch_labels = None
depends_on = None


def upgrade():
    # Drop existing FK constraint
    op.drop_constraint(
        "fk_nodenamespace_parent",
        "nodenamespace",
        type_="foreignkey",
    )

    # Recreate FK constraint with RESTRICT on delete
    op.create_foreign_key(
        "fk_nodenamespace_parent",
        "nodenamespace",
        "nodenamespace",
        ["parent_namespace"],
        ["namespace"],
        ondelete="RESTRICT",
    )


def downgrade():
    # Drop FK constraint with RESTRICT
    op.drop_constraint(
        "fk_nodenamespace_parent",
        "nodenamespace",
        type_="foreignkey",
    )

    # Recreate FK constraint without ondelete specification
    op.create_foreign_key(
        "fk_nodenamespace_parent",
        "nodenamespace",
        "nodenamespace",
        ["parent_namespace"],
        ["namespace"],
    )
