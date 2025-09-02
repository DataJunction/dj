"""
Add service accounts columns to users table

Revision ID: 5b00137c69f9
Revises: 8eab64955a49
Create Date: 2025-09-02 01:02:24.909174+00:00
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "5b00137c69f9"
down_revision = "8eab64955a49"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("is_service_account", sa.Boolean(), nullable=True),
        )
        batch_op.add_column(
            sa.Column(
                "created_by_user_id",
                sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
                nullable=True,
            ),
        )
        batch_op.add_column(
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        )
        batch_op.add_column(
            sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        )
        batch_op.create_foreign_key(
            "fk_users_created_by_user_id_users_id",
            "users",
            ["created_by_user_id"],
            ["id"],
        )

    op.execute(
        "UPDATE users SET is_service_account = false WHERE is_service_account IS NULL",
    )

    # After the is_service_account column is backfilled, make it required going forward
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.alter_column(
            "is_service_account",
            existing_type=sa.Boolean(),
            nullable=False,
        )


def downgrade():
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_users_created_by_user_id_users_id",
            type_="foreignkey",
        )
        batch_op.drop_column("last_used_at")
        batch_op.drop_column("created_at")
        batch_op.drop_column("created_by_user_id")
        batch_op.drop_column("is_active")
