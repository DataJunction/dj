"""add create_by to nodes, node revisions, collections, and tags

Revision ID: f3c9b40deb6f
Revises: 34171c92dd6d
Create Date: 2024-08-18 00:36:51.859475+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "f3c9b40deb6f"
down_revision = "34171c92dd6d"
branch_labels = None
depends_on = None


def upgrade():
    """This upgrades adds the created_by_id field to Collection, Node, NodeRevision, and Tag

    Deployments prior to this upgrade will not have easily accessible information for who created
    these objects in the past. Therefore, this upgrade backfills all existing objects as created by
    the user with ID=1. If creation information can be gathered elsewhere, it's recommended that you
    manually backfill the correct created_by_id after the database is upgraded.
    """
    with op.batch_alter_table("collection", schema=None) as batch_op:
        batch_op.add_column(sa.Column("created_by_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(None, "users", ["created_by_id"], ["id"])

    with op.batch_alter_table("node", schema=None) as batch_op:
        batch_op.add_column(sa.Column("created_by_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(None, "users", ["created_by_id"], ["id"])

    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.add_column(sa.Column("created_by_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(None, "users", ["created_by_id"], ["id"])

    with op.batch_alter_table("tag", schema=None) as batch_op:
        batch_op.add_column(sa.Column("created_by_id", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(None, "users", ["created_by_id"], ["id"])

    # Backfill created by columns with user id 1
    op.execute("UPDATE collection SET created_by_id = 1 WHERE created_by_id IS NULL")
    op.execute("UPDATE node SET created_by_id = 1 WHERE created_by_id IS NULL")
    op.execute("UPDATE noderevision SET created_by_id = 1 WHERE created_by_id IS NULL")
    op.execute("UPDATE tag SET created_by_id = 1 WHERE created_by_id IS NULL")

    # After the created by column is backfilled, make it required going forward
    with op.batch_alter_table("collection", schema=None) as batch_op:
        batch_op.alter_column(
            "created_by_id",
            existing_type=sa.Integer(),
            nullable=False,
        )

    with op.batch_alter_table("node", schema=None) as batch_op:
        batch_op.alter_column(
            "created_by_id",
            existing_type=sa.Integer(),
            nullable=False,
        )

    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.alter_column(
            "created_by_id",
            existing_type=sa.Integer(),
            nullable=False,
        )

    with op.batch_alter_table("tag", schema=None) as batch_op:
        batch_op.alter_column(
            "created_by_id",
            existing_type=sa.Integer(),
            nullable=False,
        )


def downgrade():
    with op.batch_alter_table("tag", schema=None) as batch_op:
        batch_op.drop_constraint(None, type_="foreignkey")
        batch_op.drop_column("created_by_id")

    with op.batch_alter_table("noderevision", schema=None) as batch_op:
        batch_op.drop_constraint(None, type_="foreignkey")
        batch_op.drop_column("created_by_id")

    with op.batch_alter_table("node", schema=None) as batch_op:
        batch_op.drop_constraint(None, type_="foreignkey")
        batch_op.drop_column("created_by_id")

    with op.batch_alter_table("collection", schema=None) as batch_op:
        batch_op.drop_constraint(None, type_="foreignkey")
        batch_op.drop_column("created_by_id")
