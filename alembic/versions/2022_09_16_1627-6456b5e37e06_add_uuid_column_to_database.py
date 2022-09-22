"""Add UUID column to database

Revision ID: 6456b5e37e06
Revises: 37cd6f86330a
Create Date: 2022-09-16 16:27:45.592409+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import

from uuid import UUID, uuid4

import sqlalchemy as sa
import sqlalchemy_utils
import sqlmodel
from sqlmodel import Field, Session, SQLModel

from alembic import op

# revision identifiers, used by Alembic.
revision = "6456b5e37e06"
down_revision = "37cd6f86330a"
branch_labels = None
depends_on = None

DJ_DATABASE_ID = 0
DJ_DATABASE_UUID = UUID("a59d03c7-bb00-469d-88fc-f60aff36aa3b")
SQLITE_DATABASE_ID = -1
SQLITE_DATABASE_UUID = UUID("5918965d-0fb4-429a-8ca7-9b4794dbb9c1")


class Database(SQLModel):

    """
    Mock for the ``Database`` model.
    """

    id: int
    uuid: UUID = Field(
        default_factory=uuid4,
        sa_column=SqlaColumn(UUIDType(), primary_key=True),
    )


def upgrade():
    op.alter_column("column", "id", existing_type=sa.INTEGER(), autoincrement=True)

    op.add_column(
        "database",
        sa.Column("uuid", sqlalchemy_utils.types.uuid.UUIDType(), nullable=True),
    )
    bind = op.get_bind()
    session = Session(bind=bind)
    for database in session.query(Database):
        if database.id == DJ_DATABASE_ID:
            database.uuid = DJ_DATABASE_UUID
        if database.id == SQLITE_DATABASE_ID:
            database.uuid = SQLITE_DATABASE_UUID
        else:
            database.uuid = uuid4()
        session.add(database)
        session.commit()
    op.alter_column("database", "uuid", nullable=False)

    op.alter_column(
        "database",
        "id",
        existing_type=sa.INTEGER(),
        nullable=True,
        autoincrement=True,
    )
    op.create_index(op.f("ix_database_uuid"), "database", ["uuid"], unique=False)
    op.alter_column(
        "node",
        "id",
        existing_type=sa.INTEGER(),
        nullable=True,
        autoincrement=True,
        existing_server_default=sa.text("nextval('node_id_seq'::regclass)"),
    )
    op.alter_column("nodecolumns", "node_id", existing_type=sa.INTEGER(), nullable=True)
    op.alter_column(
        "nodecolumns",
        "column_id",
        existing_type=sa.INTEGER(),
        nullable=True,
    )
    op.alter_column(
        "noderelationship",
        "parent_id",
        existing_type=sa.INTEGER(),
        nullable=True,
    )
    op.alter_column(
        "noderelationship",
        "child_id",
        existing_type=sa.INTEGER(),
        nullable=True,
    )
    op.alter_column(
        "table",
        "id",
        existing_type=sa.INTEGER(),
        nullable=True,
        autoincrement=True,
        existing_server_default=sa.text("nextval('table_id_seq'::regclass)"),
    )
    op.alter_column(
        "tablecolumns",
        "table_id",
        existing_type=sa.INTEGER(),
        nullable=True,
    )
    op.alter_column(
        "tablecolumns",
        "column_id",
        existing_type=sa.INTEGER(),
        nullable=True,
    )


def downgrade():
    op.alter_column(
        "tablecolumns",
        "column_id",
        existing_type=sa.INTEGER(),
        nullable=False,
    )
    op.alter_column(
        "tablecolumns",
        "table_id",
        existing_type=sa.INTEGER(),
        nullable=False,
    )
    op.alter_column(
        "table",
        "id",
        existing_type=sa.INTEGER(),
        nullable=False,
        autoincrement=True,
        existing_server_default=sa.text("nextval('table_id_seq'::regclass)"),
    )
    op.alter_column(
        "noderelationship",
        "child_id",
        existing_type=sa.INTEGER(),
        nullable=False,
    )
    op.alter_column(
        "noderelationship",
        "parent_id",
        existing_type=sa.INTEGER(),
        nullable=False,
    )
    op.alter_column(
        "nodecolumns",
        "column_id",
        existing_type=sa.INTEGER(),
        nullable=False,
    )
    op.alter_column(
        "nodecolumns",
        "node_id",
        existing_type=sa.INTEGER(),
        nullable=False,
    )
    op.alter_column(
        "node",
        "id",
        existing_type=sa.INTEGER(),
        nullable=False,
        autoincrement=True,
        existing_server_default=sa.text("nextval('node_id_seq'::regclass)"),
    )
    op.drop_index(op.f("ix_database_uuid"), table_name="database")
    op.alter_column(
        "database",
        "id",
        existing_type=sa.INTEGER(),
        nullable=False,
        autoincrement=True,
    )
    op.drop_column("database", "uuid")
    op.alter_column(
        "column",
        "id",
        existing_type=sa.INTEGER(),
        nullable=False,
        autoincrement=True,
    )
