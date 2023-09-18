"""Add measures

Revision ID: f2e9ef937daf
Revises: fe8d3dbe512a
Create Date: 2023-09-18 13:46:17.700118+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "f2e9ef937daf"
down_revision = "fe8d3dbe512a"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "measures",
        sa.Column(
            "additive",
            sa.Enum(
                "ADDITIVE",
                "NON_ADDITIVE",
                "SEMI_ADDITIVE",
                name="aggregationrule",
            ),
            nullable=True,
        ),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_measures")),
        sa.UniqueConstraint("name", name=op.f("uq_measures_name")),
    )
    with op.batch_alter_table("column", schema=None) as batch_op:
        batch_op.add_column(sa.Column("measure_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(
            batch_op.f("fk_column_measure_id_measures"),
            "measures",
            ["measure_id"],
            ["id"],
        )


def downgrade():
    with op.batch_alter_table("column", schema=None) as batch_op:
        batch_op.drop_constraint(
            batch_op.f("fk_column_measure_id_measures"),
            type_="foreignkey",
        )
        batch_op.drop_column("measure_id")

    op.drop_table("measures")
