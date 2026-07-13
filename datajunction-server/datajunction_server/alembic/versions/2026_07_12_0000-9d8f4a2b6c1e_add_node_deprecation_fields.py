"""
Add node deprecation fields

Adds deprecated_at and succeeded_by_id columns to the node table to support
node deprecation, which is distinct from deactivation: a deprecated node
still resolves and is served, but signals to consumers that they should
migrate off of it, optionally to a single named successor node.

Revision ID: 9d8f4a2b6c1e
Revises: 3c7a9f1b2e4d
Create Date: 2026-07-12 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9d8f4a2b6c1e"
down_revision = "3c7a9f1b2e4d"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "node",
        sa.Column("deprecated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "node",
        sa.Column("succeeded_by_id", sa.BigInteger(), nullable=True),
    )
    op.create_foreign_key(
        "fk_node_succeeded_by_id",
        "node",
        "node",
        ["succeeded_by_id"],
        ["id"],
    )


def downgrade():
    op.drop_constraint(
        "fk_node_succeeded_by_id",
        "node",
        type_="foreignkey",
    )
    op.drop_column("node", "succeeded_by_id")
    op.drop_column("node", "deprecated_at")
