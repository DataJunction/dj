"""catalog and table relationships

Revision ID: e19b5daa77a7
Revises: 2bdcec2d24a7
Create Date: 2023-02-10 03:45:17.696922+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlalchemy_utils
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "e19b5daa77a7"
down_revision = "2bdcec2d24a7"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("catalog") as batch_op:
        batch_op.add_column(
            sa.Column("uuid", sqlalchemy_utils.types.uuid.UUIDType(), nullable=True),
        )
        batch_op.add_column(
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        )
        batch_op.add_column(
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        )
        batch_op.add_column(sa.Column("extra_params", sa.JSON(), nullable=True))

    with op.batch_alter_table("engine") as batch_op:
        batch_op.add_column(
            sa.Column("uri", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        )

    with op.batch_alter_table("table") as batch_op:
        batch_op.add_column(sa.Column("catalog_id", sa.Integer(), nullable=True))
        batch_op.drop_column("catalog")
        batch_op.create_foreign_key(
            op.f("fk_table_catalog_id_catalog"),
            "catalog",
            ["catalog_id"],
            ["id"],
        )


def downgrade():
    with op.batch_alter_table("catalog") as batch_op:
        batch_op.drop_column("extra_params")
        batch_op.drop_column("updated_at")
        batch_op.drop_column("created_at")
        batch_op.drop_column("uuid")

    with op.batch_alter_table("engine") as batch_op:
        batch_op.drop_column("uri")

    with op.batch_alter_table("table") as batch_op:
        batch_op.add_column(sa.Column("catalog", sa.VARCHAR(), nullable=True))
        batch_op.drop_column("catalog_id")
        batch_op.drop_constraint(
            op.f("fk_table_catalog_id_catalog"),
            type_="foreignkey",
        )
