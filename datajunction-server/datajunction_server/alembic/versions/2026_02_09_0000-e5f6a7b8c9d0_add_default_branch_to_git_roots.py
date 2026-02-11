"""
Add default_branch field to nodenamespace for git root namespaces

Adds default_branch column to support specifying which branch to use as the
source when creating new branches from a git root namespace. This allows users
to create branches from git root namespaces in the UI.

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-02-09 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e5f6a7b8c9d0"
down_revision = "d4e5f6a7b8c9"
branch_labels = None
depends_on = None


def upgrade():
    # Add default_branch column to nodenamespace
    op.add_column(
        "nodenamespace",
        sa.Column("default_branch", sa.String(), nullable=True),
    )


def downgrade():
    op.drop_column("nodenamespace", "default_branch")
