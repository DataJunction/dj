"""add warnings to deployments

Revision ID: dw0001warnings
Revises: rbacenumfix1
Create Date: 2026-08-05 00:00:00.000000+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op

revision = "dw0001warnings"
down_revision = "rbacenumfix1"
branch_labels = None
depends_on = None


def upgrade():
    # Warnings accumulated during a deploy, so they surface even when it succeeds.
    op.add_column(
        "deployments",
        sa.Column("warnings", sa.JSON(), nullable=True),
    )


def downgrade():
    op.drop_column("deployments", "warnings")
