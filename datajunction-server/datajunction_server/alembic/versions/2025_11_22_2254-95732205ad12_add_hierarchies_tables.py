"""Add hierarchies tables

Revision ID: 95732205ad12
Revises: be76e22dd71a
Create Date: 2025-11-22 22:54:00.000000+00:00
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "95732205ad12"
down_revision = "be76e22dd71a"
branch_labels = None
depends_on = None


def upgrade():
    # Create hierarchies table
    op.create_table(
        "hierarchies",
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
        ),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("display_name", sa.String(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_by_id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["created_by_id"],
            ["users.id"],
            name="fk_hierarchies_created_by_id_users",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_hierarchies"),
        sa.UniqueConstraint("name", name="uq_hierarchies_name"),
    )

    # Create hierarchy_levels table
    op.create_table(
        "hierarchy_levels",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("hierarchy_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("dimension_node_id", sa.BigInteger(), nullable=False),
        sa.Column("level_order", sa.Integer(), nullable=False),
        sa.Column("grain_columns", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(
            ["hierarchy_id"],
            ["hierarchies.id"],
            name="fk_hierarchy_levels_hierarchy_id_hierarchies",
        ),
        sa.ForeignKeyConstraint(
            ["dimension_node_id"],
            ["node.id"],
            name="fk_hierarchy_levels_dimension_node_id_node",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_hierarchy_levels"),
        sa.UniqueConstraint(
            "hierarchy_id",
            "name",
            name="hierarchy_levels_hierarchy_id_name_key",
        ),
    )


def downgrade():
    # Drop hierarchy_levels first (has foreign key to hierarchies)
    op.drop_table("hierarchy_levels")

    # Drop hierarchies table
    op.drop_table("hierarchies")
