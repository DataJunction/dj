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
    principalkind = sa.Enum("USER", "SERVICE_ACCOUNT", name="principalkind")
    principalkind.create(
        op.get_bind(),
        checkfirst=True,
    )

    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.add_column(sa.Column("kind", principalkind, nullable=True))
        batch_op.add_column(
            sa.Column(
                "created_by_id",
                sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
                nullable=True,
            ),
        )
        batch_op.add_column(
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        )
        batch_op.create_foreign_key(
            "fk_users_created_by_id_users_id",
            "users",
            ["created_by_id"],
            ["id"],
        )

    # Backfill existing users to not be service accounts
    op.execute(
        "UPDATE users SET kind = 'USER' WHERE kind IS NULL",
    )

    # After the `kind`` column is backfilled, make it required going forward
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.alter_column("kind", nullable=False)


def downgrade():
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_users_created_by_id_users_id",
            type_="foreignkey",
        )
        batch_op.drop_column("created_at")
        batch_op.drop_column("created_by_id")
        batch_op.drop_column("kind")

    principalkind = sa.Enum("USER", "SERVICE_ACCOUNT", name="principalkind")
    principalkind.drop(op.get_bind(), checkfirst=True)
