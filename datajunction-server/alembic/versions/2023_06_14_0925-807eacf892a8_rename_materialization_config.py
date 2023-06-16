"""Rename materialization config

Revision ID: 807eacf892a8
Revises: 07aa35bab455
Create Date: 2023-06-14 09:25:34.860520+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel
from sqlalchemy.dialects import sqlite

from alembic import op

# revision identifiers, used by Alembic.
revision = "807eacf892a8"
down_revision = "07aa35bab455"
branch_labels = None
depends_on = None


def upgrade():
    op.rename_table("materializationconfig", "materialization")


def downgrade():
    op.rename_table("materialization", "materializationconfig")
