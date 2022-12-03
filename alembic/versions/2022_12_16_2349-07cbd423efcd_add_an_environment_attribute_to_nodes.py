"""Add an environment attribute to nodes

Revision ID: 07cbd423efcd
Revises: f620c4521c80
Create Date: 2022-12-16 23:49:44.715585+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "07cbd423efcd"
down_revision = "f620c4521c80"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("node", sa.Column("environment", sa.String))


def downgrade():
    op.drop_column("node", "environment")
