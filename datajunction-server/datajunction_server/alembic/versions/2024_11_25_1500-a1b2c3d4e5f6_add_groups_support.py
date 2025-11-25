"""
Add groups support

Revision ID: a1b2c3d4e5f6
Revises: be76e22dd71a
Create Date: 2024-11-25 15:00:00.000000+00:00
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "be76e22dd71a"
branch_labels = None
depends_on = None


def upgrade():
    # 1. Add GROUP to PrincipalKind enum
    # Note: Postgres doesn't support adding enum values in a transaction,
    # so we use ALTER TYPE directly
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE principalkind ADD VALUE IF NOT EXISTS 'GROUP'")

    # 2. Create group_members table
    op.create_table(
        "group_members",
        sa.Column(
            "group_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "member_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.ForeignKeyConstraint(
            ["group_id"],
            ["users.id"],
            name="fk_group_members_group_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["member_id"],
            ["users.id"],
            name="fk_group_members_member_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("group_id", "member_id"),
        sa.CheckConstraint(
            "group_id != member_id",
            name="chk_no_self_membership",
        ),
    )

    # 3. Create indexes for better query performance
    op.create_index(
        "idx_group_members_group_id",
        "group_members",
        ["group_id"],
    )
    op.create_index(
        "idx_group_members_member_id",
        "group_members",
        ["member_id"],
    )


def downgrade():
    # Drop indexes
    op.drop_index("idx_group_members_member_id", table_name="group_members")
    op.drop_index("idx_group_members_group_id", table_name="group_members")

    # Drop group_members table
    op.drop_table("group_members")

    # Note: Postgres doesn't support removing enum values easily
    # Users will need to manually handle enum cleanup if needed
    # Or recreate the enum without GROUP value
