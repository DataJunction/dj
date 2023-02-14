"""Add materialization config

Revision ID: f3cbcd8dfac2
Revises: b9f370ef912a
Create Date: 2023-02-11 18:31:49.245407+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "f3cbcd8dfac2"
down_revision = "6bbaf7974a9d"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "materializationconfig",
        sa.Column("node_revision_id", sa.Integer(), nullable=False),
        sa.Column("engine_id", sa.Integer(), nullable=False),
        sa.Column("config", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.ForeignKeyConstraint(
            ["engine_id"],
            ["engine.id"],
            name=op.f("fk_materializationconfig_engine_id_engine"),
        ),
        sa.ForeignKeyConstraint(
            ["node_revision_id"],
            ["noderevision.id"],
            name=op.f("fk_materializationconfig_node_revision_id_noderevision"),
        ),
        sa.PrimaryKeyConstraint(
            "node_revision_id",
            "engine_id",
            name=op.f("pk_materializationconfig"),
        ),
    )

    with op.batch_alter_table("noderevision") as batch_op:
        batch_op.create_unique_constraint(
            op.f("uq_noderevision_version"),
            ["version", "node_id"],
        )


def downgrade():
    op.drop_table("materializationconfig")

    with op.batch_alter_table("noderevision") as batch_op:
        batch_op.drop_constraint("uq_noderevision_version")
