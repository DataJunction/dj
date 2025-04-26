"""fix unique constraint

Revision ID: 51547dcccb10
Revises: a2d7ea04cf79
Create Date: 2025-04-24 18:10:33.759611+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op


# revision identifiers, used by Alembic.
revision = "51547dcccb10"
down_revision = "a2d7ea04cf79"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("notificationpreferences", schema=None) as batch_op:
        batch_op.drop_constraint("uix_entity_type_name", type_="unique")
        batch_op.create_unique_constraint(
            "uix_user_entity_type_name",
            ["user_id", "entity_type", "entity_name"],
        )


def downgrade():
    with op.batch_alter_table("notificationpreferences", schema=None) as batch_op:
        batch_op.drop_constraint("uix_user_entity_type_name", type_="unique")
        batch_op.create_unique_constraint(
            "uix_entity_type_name",
            ["entity_name", "entity_type"],
        )
