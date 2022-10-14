"""Add extra_params to database

Revision ID: 7f8367fdfa30
Revises: 5f31ff8814e7
Create Date: 2022-04-30 20:22:33.502343+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "7f8367fdfa30"
down_revision = "5f31ff8814e7"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("database", sa.Column("extra_params", sa.JSON(), nullable=True))


def downgrade():
    with op.batch_alter_table("database") as bop:
        bop.drop_column("extra_params")
