"""node ruleset verdict table

Revision ID: rv0001verdicts
Revises: dv0001typed
Create Date: 2026-09-27 00:00:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op

revision = "rv0001verdicts"
down_revision = "dv0001typed"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "noderulesetverdict",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("node_id", sa.BigInteger(), nullable=False),
        sa.Column("ruleset", sa.String(), nullable=False),
        sa.Column("verdict", sa.String(), nullable=False),
        sa.Column("node_version", sa.String(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["node_id"], ["node.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("node_id", "ruleset"),
    )
    op.create_index(
        "ix_noderulesetverdict_node_id",
        "noderulesetverdict",
        ["node_id"],
    )
    # The list filter asks for one ruleset's verdict across many nodes, so the
    # pair is what gets scanned rather than either column alone.
    op.create_index(
        "ix_noderulesetverdict_ruleset_verdict",
        "noderulesetverdict",
        ["ruleset", "verdict"],
    )


def downgrade():
    op.drop_index(
        "ix_noderulesetverdict_ruleset_verdict",
        table_name="noderulesetverdict",
    )
    op.drop_index(
        "ix_noderulesetverdict_node_id",
        table_name="noderulesetverdict",
    )
    op.drop_table("noderulesetverdict")
