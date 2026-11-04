"""store dimension link default values as json

Revision ID: dv0001typed
Revises: fg0001fixedgrain
Create Date: 2026-09-23 00:00:00.000000+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring

import json

import sqlalchemy as sa
from alembic import op

revision = "dv0001typed"
down_revision = "fg0001fixedgrain"
branch_labels = None
depends_on = None


def _rewrite_values(convert):
    """Rewrite every stored default through convert()."""
    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            "SELECT id, default_value FROM dimensionlink "
            "WHERE default_value IS NOT NULL",
        ),
    ).fetchall()
    for link_id, value in rows:
        bind.execute(
            sa.text(
                "UPDATE dimensionlink SET default_value = :value WHERE id = :id",
            ),
            {"value": convert(value), "id": link_id},
        )


def upgrade():
    if op.get_bind().dialect.name == "postgresql":
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
    # Existing values are bare text; store them as JSON strings.
    _rewrite_values(json.dumps)


def downgrade():
    if op.get_bind().dialect.name == "postgresql":
        op.execute(
            "ALTER TABLE dimensionlink "
            "ALTER COLUMN default_value TYPE varchar "
            "USING default_value #>> '{}'",
        )
        return

    _rewrite_values(lambda value: str(json.loads(value)))
    with op.batch_alter_table("dimensionlink", schema=None) as batch_op:
        batch_op.alter_column(
            "default_value",
            existing_type=sa.JSON(),
            type_=sa.String(),
            existing_nullable=True,
        )
