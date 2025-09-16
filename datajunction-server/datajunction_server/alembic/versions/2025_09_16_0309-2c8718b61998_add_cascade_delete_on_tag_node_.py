"""
Add cascade delete on tag node relationship

Revision ID: 2c8718b61998
Revises: b6398ba852b3
Create Date: 2025-09-16 03:09:37.801249+00:00
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op

# revision identifiers, used by Alembic.
revision = "2c8718b61998"
down_revision = "b6398ba852b3"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("tagnoderelationship", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_tagnoderelationship_node_id_node",
            type_="foreignkey",
        )
        batch_op.drop_constraint(
            "fk_tagnoderelationship_tag_id_tag",
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_tagnoderelationship_node_id_node",
            "node",
            ["node_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            "fk_tagnoderelationship_tag_id_tag",
            "tag",
            ["tag_id"],
            ["id"],
            ondelete="CASCADE",
        )


def downgrade():
    with op.batch_alter_table("tagnoderelationship", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_tagnoderelationship_tag_id_tag",
            type_="foreignkey",
        )
        batch_op.drop_constraint(
            "fk_tagnoderelationship_node_id_node",
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_tagnoderelationship_tag_id_tag",
            "tag",
            ["tag_id"],
            ["id"],
        )
        batch_op.create_foreign_key(
            "fk_tagnoderelationship_node_id_node",
            "node",
            ["node_id"],
            ["id"],
        )
