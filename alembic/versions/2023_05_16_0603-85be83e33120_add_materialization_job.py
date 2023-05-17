"""
Add materialization job

Revision ID: f8c3e437261b
Revises: e41c021c19a6
Create Date: 2023-05-09 14:30:22.729559+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "f8c3e437261b"
down_revision = "980cc03c5242"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("materializationconfig", sa.Column("job", sa.String(), nullable=True))


def downgrade():
    op.drop_column("materializationconfig", "job")
