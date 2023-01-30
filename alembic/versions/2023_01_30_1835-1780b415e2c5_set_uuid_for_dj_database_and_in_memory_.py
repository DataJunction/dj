"""Set uuid for dj database and in-memory database

Revision ID: 1780b415e2c5
Revises: af60d8073876
Create Date: 2023-01-30 18:35:49.406002+00:00

"""
# pylint: disable=invalid-name

from uuid import UUID

import sqlalchemy as sa
import sqlalchemy_utils
from sqlalchemy import table

from alembic import op

# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module


# revision identifiers, used by Alembic.
revision = "1780b415e2c5"
down_revision = "af60d8073876"
branch_labels = None
depends_on = None

DJ_DATABASE_ID = 0
DJ_DATABASE_UUID = UUID("594804bf-47cb-426c-83c4-94a348e95972")
SQLITE_DATABASE_ID = -1
SQLITE_DATABASE_UUID = UUID("3619eeba-d628-4ab1-9dd5-65738ab3c02f")


def upgrade():
    database = table(
        "database",
        sa.Column("id", sa.Integer()),
        sa.Column("uuid", sqlalchemy_utils.types.uuid.UUIDType(), nullable=True),
    )
    op.execute(
        database.update()
        .where(database.c.id == DJ_DATABASE_ID)
        .values({"uuid": DJ_DATABASE_UUID}),
    )
    op.execute(
        database.update()
        .where(database.c.id == SQLITE_DATABASE_ID)
        .values({"uuid": SQLITE_DATABASE_UUID}),
    )


def downgrade():
    database = table(
        "database",
        sa.Column("id", sa.Integer()),
        sa.Column("uuid", sqlalchemy_utils.types.uuid.UUIDType(), nullable=True),
    )
    op.execute(
        database.update().where(database.c.id == DJ_DATABASE_ID).values({"uuid": None}),
    )
    op.execute(
        database.update()
        .where(database.c.id == SQLITE_DATABASE_ID)
        .values({"uuid": None}),
    )
