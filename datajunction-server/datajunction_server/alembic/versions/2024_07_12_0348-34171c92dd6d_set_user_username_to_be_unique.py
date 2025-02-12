"""Set User.username to be unique

Revision ID: 34171c92dd6d
Revises: 640a814db2d8
Create Date: 2024-07-12 03:48:53.609685+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op

# revision identifiers, used by Alembic.
revision = "34171c92dd6d"
down_revision = "640a814db2d8"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.create_unique_constraint(None, ["username"])


def downgrade():
    with op.batch_alter_table("users", schema=None) as batch_op:
        batch_op.drop_constraint(None, type_="unique")
