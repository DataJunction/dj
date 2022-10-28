"""Add UUID column to database model

Revision ID: f620c4521c80
Revises: 71b291295ae1
Create Date: 2022-10-27 22:42:19.160413+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from uuid import UUID

import sqlalchemy as sa
import sqlalchemy_utils
import sqlmodel
from sqlalchemy.sql import table

from alembic import op

# revision identifiers, used by Alembic.
revision = "f620c4521c80"
down_revision = "71b291295ae1"
branch_labels = None
depends_on = None


DJ_DATABASE_ID = 0
DJ_DATABASE_UUID = UUID("594804bf-47cb-426c-83c4-94a348e95972")
SQLITE_DATABASE_ID = -1
SQLITE_DATABASE_UUID = UUID("3619eeba-d628-4ab1-9dd5-65738ab3c02f")


def upgrade():
    with op.batch_alter_table("database") as bop:
        bop.add_column(
            sa.Column("uuid", sqlalchemy_utils.types.uuid.UUIDType(), nullable=True),
        )

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
    with op.batch_alter_table("database") as bop:
        bop.drop_column("uuid")
