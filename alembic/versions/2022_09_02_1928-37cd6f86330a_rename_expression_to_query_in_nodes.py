"""Rename expression to query in nodes

Revision ID: 37cd6f86330a
Revises: 7f8367fdfa30
Create Date: 2022-09-02 19:28:11.904941+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "37cd6f86330a"
down_revision = "7f8367fdfa30"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_index("ix_node_expression", table_name="node")
    with op.batch_alter_table("node") as bop:
        bop.alter_column("expression", new_column_name="query")
    op.create_index(op.f("ix_node_query"), "node", ["query"], unique=False)


def downgrade():
    op.drop_index(op.f("ix_node_query"), table_name="node")
    with op.batch_alter_table("node") as bop:
        bop.alter_column("query", new_column_name="expression")
    op.create_index("ix_node_expression", "node", ["expression"], unique=False)
