"""
Add indexes to deployments table

Revision ID: f6562450c2c7
Revises: 5a6b7c8d9e0f
Create Date: 2026-01-13 01:56:14.970591+00:00
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op

# revision identifiers, used by Alembic.
revision = "f6562450c2c7"
down_revision = "5a6b7c8d9e0f"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("deployments", schema=None) as batch_op:
        batch_op.create_index("ix_deployments_namespace", ["namespace"], unique=False)
        batch_op.create_index(
            "ix_deployments_namespace_status_created",
            ["namespace", "status", "created_at"],
            unique=False,
        )


def downgrade():
    with op.batch_alter_table("deployments", schema=None) as batch_op:
        batch_op.drop_index("ix_deployments_namespace_status_created")
        batch_op.drop_index("ix_deployments_namespace")
