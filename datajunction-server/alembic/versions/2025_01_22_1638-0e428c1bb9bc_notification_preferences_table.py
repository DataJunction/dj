"""notification_preferences table

Revision ID: 0e428c1bb9bc
Revises: 70904373eab3
Create Date: 2025-01-22 16:38:57.407898+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "0e428c1bb9bc"
down_revision = "70904373eab3"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "notification_preferences",
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column(
            "entity_type",
            sa.Enum(
                "ATTRIBUTE",
                "AVAILABILITY",
                "BACKFILL",
                "CATALOG",
                "COLUMN_ATTRIBUTE",
                "DEPENDENCY",
                "ENGINE",
                "LINK",
                "MATERIALIZATION",
                "NAMESPACE",
                "NODE",
                "PARTITION",
                "QUERY",
                "TAG",
                name="notification_entitytype",
            ),
            nullable=False,
        ),
        sa.Column("entity_name", sa.String(), nullable=False),
        sa.Column(
            "activity_types",
            sa.ARRAY(
                sa.Enum(
                    "CREATE",
                    "DELETE",
                    "RESTORE",
                    "UPDATE",
                    "REFRESH",
                    "TAG",
                    "SET_ATTRIBUTE",
                    "STATUS_CHANGE",
                    name="notification_activitytype",
                ),
            ),
            nullable=False,
        ),
        sa.Column(
            "user_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column("alert_types", sa.ARRAY(sa.String()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade():
    op.drop_table("notification_preferences")
