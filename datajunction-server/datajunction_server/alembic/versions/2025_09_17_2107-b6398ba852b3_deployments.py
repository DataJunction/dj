"""
Deployments

Revision ID: b6398ba852b3
Revises: b55add7e1ebc
Create Date: 2025-09-12 00:07:30.531304+00:00
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op
import sqlalchemy_utils

# revision identifiers, used by Alembic.
revision = "b6398ba852b3"
down_revision = "b55add7e1ebc"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "deployments",
        sa.Column("uuid", sqlalchemy_utils.types.uuid.UUIDType(), nullable=False),
        sa.Column("namespace", sa.String(), nullable=False),
        sa.Column(
            "status",
            sa.Enum("PENDING", "RUNNING", "FAILED", "SUCCESS", name="deploymentstatus"),
            nullable=False,
        ),
        sa.Column("spec", sa.JSON(), nullable=False),
        sa.Column("results", sa.JSON(), nullable=False),
        sa.Column(
            "created_by_id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["created_by_id"],
            ["users.id"],
        ),
        sa.PrimaryKeyConstraint("uuid"),
    )


def downgrade():
    op.drop_table("deployments")
