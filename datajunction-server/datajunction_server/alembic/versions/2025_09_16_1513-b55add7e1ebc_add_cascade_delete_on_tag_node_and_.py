"""
Add cascade delete on tag-node and column-attr foreign keys
Revision ID: b55add7e1ebc
Revises: 759c4d50cb8d
Create Date: 2025-09-16 15:13:39.963297+00:00
"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op

# revision identifiers, used by Alembic.
revision = "b55add7e1ebc"
down_revision = "759c4d50cb8d"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("columnattribute", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_columnattribute_attribute_type_id_attributetype",
            type_="foreignkey",
        )
        batch_op.drop_constraint(
            "fk_columnattribute_column_id_column",
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_columnattribute_attribute_type_id_attributetype",
            "attributetype",
            ["attribute_type_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            "fk_columnattribute_column_id_column",
            "column",
            ["column_id"],
            ["id"],
            ondelete="CASCADE",
        )

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
            "fk_tagnoderelationship_tag_id_tag",
            "tag",
            ["tag_id"],
            ["id"],
            ondelete="CASCADE",
        )
        batch_op.create_foreign_key(
            "fk_tagnoderelationship_node_id_node",
            "node",
            ["node_id"],
            ["id"],
            ondelete="CASCADE",
        )


def downgrade():
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

    with op.batch_alter_table("columnattribute", schema=None) as batch_op:
        batch_op.drop_constraint(
            "fk_columnattribute_column_id_column",
            type_="foreignkey",
        )
        batch_op.drop_constraint(
            "fk_columnattribute_attribute_type_id_attributetype",
            type_="foreignkey",
        )
        batch_op.create_foreign_key(
            "fk_columnattribute_column_id_column",
            "column",
            ["column_id"],
            ["id"],
        )
        batch_op.create_foreign_key(
            "fk_columnattribute_attribute_type_id_attributetype",
            "attributetype",
            ["attribute_type_id"],
            ["id"],
        )
