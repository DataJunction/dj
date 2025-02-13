"""notificationpreferences

Revision ID: c3d5f327296c
Revises: bec3296d7537
Create Date: 2025-02-11 16:19:35.918790+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "c3d5f327296c"
down_revision = "bec3296d7537"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "notificationpreferences",
        sa.Column("id", sa.BigInteger(), nullable=False),
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
        sa.UniqueConstraint("entity_name", "entity_type", name="uix_entity_type_name"),
    )


def downgrade():
    op.drop_table("notificationpreferences")
