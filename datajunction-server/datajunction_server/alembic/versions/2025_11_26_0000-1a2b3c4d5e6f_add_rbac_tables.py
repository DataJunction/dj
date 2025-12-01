"""
Add RBAC tables

Revision ID: 1a2b3c4d5e6f
Revises: a1b2c3d4e5f6
Create Date: 2025-11-26 00:00:00.000000+00:00
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "1a2b3c4d5e6f"
down_revision = (
    "a1b2c3d4e5f6",
    "95732205ad12",
)  # Merge groups and hierarchies branches
branch_labels = None
depends_on = None


def upgrade():
    # Define enums for use in table definitions
    # Note: The enums will be created automatically by SQLAlchemy when used in columns
    action_enum = sa.Enum(
        "read",
        "write",
        "execute",
        "delete",
        "manage",
        name="resourceaction",
    )
    resource_type_enum = sa.Enum("node", "namespace", name="resourcetype")

    # Create roles table
    op.create_table(
        "roles",
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "created_by_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "deleted_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["created_by_id"],
            ["users.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_index("idx_roles_name", "roles", ["name"])
    op.create_index("idx_roles_created_by", "roles", ["created_by_id"])
    op.create_index("idx_roles_deleted_at", "roles", ["deleted_at"])

    # Create role_scopes table
    op.create_table(
        "role_scopes",
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "role_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column("action", action_enum, nullable=False),
        sa.Column("scope_type", resource_type_enum, nullable=False),
        sa.Column("scope_value", sa.String(500), nullable=False),
        sa.ForeignKeyConstraint(
            ["role_id"],
            ["roles.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_role_scopes_role_id", "role_scopes", ["role_id"])
    op.create_index(
        "idx_unique_role_scope",
        "role_scopes",
        ["role_id", "action", "scope_type", "scope_value"],
        unique=True,
    )

    # Create role_assignments table
    op.create_table(
        "role_assignments",
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "principal_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "role_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "granted_by_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "granted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["principal_id"],
            ["users.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["role_id"],
            ["roles.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["granted_by_id"],
            ["users.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_role_assignments_principal",
        "role_assignments",
        ["principal_id"],
    )
    op.create_index("idx_role_assignments_role", "role_assignments", ["role_id"])
    op.create_index(
        "idx_unique_role_assignment",
        "role_assignments",
        ["principal_id", "role_id"],
        unique=True,
    )


def downgrade():
    # Drop tables in reverse order
    op.drop_index("idx_unique_role_assignment", table_name="role_assignments")
    op.drop_index("idx_role_assignments_role", table_name="role_assignments")
    op.drop_index("idx_role_assignments_principal", table_name="role_assignments")
    op.drop_table("role_assignments")

    op.drop_index("idx_unique_role_scope", table_name="role_scopes")
    op.drop_index("idx_role_scopes_role_id", table_name="role_scopes")
    op.drop_table("role_scopes")

    op.drop_index("idx_roles_deleted_at", table_name="roles")
    op.drop_index("idx_roles_created_by", table_name="roles")
    op.drop_index("idx_roles_name", table_name="roles")
    op.drop_table("roles")

    # Drop enums
    resource_type_enum = sa.Enum("node", "namespace", name="resourcetype")
    resource_type_enum.drop(op.get_bind(), checkfirst=True)

    action_enum = sa.Enum(
        "read",
        "write",
        "execute",
        "delete",
        "manage",
        name="resourceaction",
    )
    action_enum.drop(op.get_bind(), checkfirst=True)
