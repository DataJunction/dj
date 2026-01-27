"""
Add git branch management fields to nodenamespace table

Adds github_repo_path, git_branch, git_path, git_only, and parent_namespace
columns to support git-backed branch management where users can create branches
and sync changes to GitHub from the UI.

If git_only is True, the namespace can only be modified through deployments
from git (via /deployments API), and UI edits are blocked.

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-01-27 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c3d4e5f6a7b8"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade():
    # Add git configuration columns to nodenamespace
    op.add_column(
        "nodenamespace",
        sa.Column("github_repo_path", sa.String(), nullable=True),
    )
    op.add_column(
        "nodenamespace",
        sa.Column("git_branch", sa.String(), nullable=True),
    )
    op.add_column(
        "nodenamespace",
        sa.Column("git_path", sa.String(), nullable=True),
    )
    op.add_column(
        "nodenamespace",
        sa.Column(
            "git_only",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
    )
    op.add_column(
        "nodenamespace",
        sa.Column("parent_namespace", sa.String(), nullable=True),
    )
    op.create_foreign_key(
        "fk_nodenamespace_parent",
        "nodenamespace",
        "nodenamespace",
        ["parent_namespace"],
        ["namespace"],
    )


def downgrade():
    op.drop_constraint(
        "fk_nodenamespace_parent",
        "nodenamespace",
        type_="foreignkey",
    )
    op.drop_column("nodenamespace", "parent_namespace")
    op.drop_column("nodenamespace", "git_only")
    op.drop_column("nodenamespace", "git_path")
    op.drop_column("nodenamespace", "git_branch")
    op.drop_column("nodenamespace", "github_repo_path")
