"""store dimension link default values as json

Revision ID: dl_default_value_json
Revises: dw0001warnings
Create Date: 2026-08-10 00:00:00.000000+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring

import sqlalchemy as sa
from alembic import op

revision = "dl_default_value_json"
down_revision = "dw0001warnings"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(
            "ALTER TABLE dimensionlink "
            "ALTER COLUMN default_value TYPE jsonb "
            "USING to_jsonb(default_value)",
        )
        return

    with op.batch_alter_table("dimensionlink", schema=None) as batch_op:
        batch_op.alter_column(
            "default_value",
            existing_type=sa.String(),
            type_=sa.JSON(),
            existing_nullable=True,
        )


def downgrade():
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.execute(
            "ALTER TABLE dimensionlink "
            "ALTER COLUMN default_value TYPE varchar "
            "USING default_value #>> '{}'",
        )
        return

    with op.batch_alter_table("dimensionlink", schema=None) as batch_op:
        batch_op.alter_column(
            "default_value",
            existing_type=sa.JSON(),
            type_=sa.String(),
            existing_nullable=True,
        )
