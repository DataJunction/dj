"""Add durable governed namespace boundaries.

Revision ID: nsboundary1
Revises: dw0001warnings
Create Date: 2026-08-11 22:00:00.000000+00:00
"""

# pylint: disable=no-member, invalid-name, missing-function-docstring

import sqlalchemy as sa
from alembic import op

revision = "nsboundary1"
down_revision = "dw0001warnings"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "namespace_boundaries",
        sa.Column("namespace", sa.String(length=500), nullable=False),
        sa.Column(
            "owner_role_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "deployer_role_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
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
        sa.ForeignKeyConstraint(
            ["owner_role_id"],
            ["roles.id"],
            name="fk_namespace_boundaries_owner_role_id_roles",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["deployer_role_id"],
            ["roles.id"],
            name="fk_namespace_boundaries_deployer_role_id_roles",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["created_by_id"],
            ["users.id"],
            name="fk_namespace_boundaries_created_by_id_users",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "owner_role_id <> deployer_role_id",
            name="ck_namespace_boundaries_distinct_roles",
        ),
        sa.PrimaryKeyConstraint("namespace"),
    )
    op.create_index(
        "idx_namespace_boundaries_owner_role_id",
        "namespace_boundaries",
        ["owner_role_id"],
        unique=True,
    )
    op.create_index(
        "idx_namespace_boundaries_deployer_role_id",
        "namespace_boundaries",
        ["deployer_role_id"],
        unique=True,
    )
    op.create_index(
        "idx_namespace_boundaries_created_by_id",
        "namespace_boundaries",
        ["created_by_id"],
    )


def downgrade():
    op.execute(
        sa.text(
            """
            CREATE TEMPORARY TABLE _dj_namespace_boundary_role_ids AS
            SELECT owner_role_id AS role_id FROM namespace_boundaries
            UNION
            SELECT deployer_role_id AS role_id FROM namespace_boundaries
            """,
        ),
    )
    op.drop_index(
        "idx_namespace_boundaries_created_by_id",
        table_name="namespace_boundaries",
    )
    op.drop_index(
        "idx_namespace_boundaries_deployer_role_id",
        table_name="namespace_boundaries",
    )
    op.drop_index(
        "idx_namespace_boundaries_owner_role_id",
        table_name="namespace_boundaries",
    )
    op.drop_table("namespace_boundaries")
    op.execute(
        sa.text(
            """
            DELETE FROM role_assignments
            WHERE role_id IN (
                SELECT role_id FROM _dj_namespace_boundary_role_ids
            )
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            DELETE FROM role_scopes
            WHERE role_id IN (
                SELECT role_id FROM _dj_namespace_boundary_role_ids
            )
            """,
        ),
    )
    op.execute(
        sa.text(
            """
            DELETE FROM roles
            WHERE id IN (
                SELECT role_id FROM _dj_namespace_boundary_role_ids
            )
            """,
        ),
    )
    op.execute(sa.text("DROP TABLE _dj_namespace_boundary_role_ids"))
