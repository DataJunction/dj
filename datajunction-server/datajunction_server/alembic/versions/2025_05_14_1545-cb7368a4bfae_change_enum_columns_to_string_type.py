"""Change enum columns to string type

Revision ID: cb7368a4bfae
Revises: 51547dcccb10
Create Date: 2025-05-14 15:45:43.734154+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "cb7368a4bfae"
down_revision = "51547dcccb10"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("history", schema=None) as batch_op:
        batch_op.alter_column(
            "entity_type",
            existing_type=postgresql.ENUM(
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
                name="entitytype",
            ),
            type_=sa.String(length=20),
            existing_nullable=True,
        )
        batch_op.alter_column(
            "activity_type",
            existing_type=postgresql.ENUM(
                "CREATE",
                "DELETE",
                "RESTORE",
                "UPDATE",
                "REFRESH",
                "TAG",
                "SET_ATTRIBUTE",
                "STATUS_CHANGE",
                name="activitytype",
            ),
            type_=sa.String(length=20),
            existing_nullable=True,
        )


def downgrade():
    with op.batch_alter_table("history", schema=None) as batch_op:
        batch_op.alter_column(
            "activity_type",
            existing_type=sa.String(length=20),
            type_=postgresql.ENUM(
                "CREATE",
                "DELETE",
                "RESTORE",
                "UPDATE",
                "REFRESH",
                "TAG",
                "SET_ATTRIBUTE",
                "STATUS_CHANGE",
                name="activitytype",
            ),
            existing_nullable=True,
        )
        batch_op.alter_column(
            "entity_type",
            existing_type=sa.String(length=20),
            type_=postgresql.ENUM(
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
                name="entitytype",
            ),
            existing_nullable=True,
        )
