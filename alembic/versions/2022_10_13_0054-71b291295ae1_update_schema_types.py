"""Update schema types

Revision ID: 71b291295ae1
Revises: 37cd6f86330a
Create Date: 2022-10-13 00:54:24.422858+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "71b291295ae1"
down_revision = "37cd6f86330a"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_index("ix_column_dimension_column", table_name="column")
    op.drop_index("ix_column_dimension_id", table_name="column")
    op.drop_index("ix_column_id", table_name="column")
    op.drop_index("ix_column_name", table_name="column")

    with op.batch_alter_table("database") as bop:
        bop.alter_column(
            "description",
            existing_type=sa.VARCHAR(),
            nullable=False,
        )
        bop.alter_column("read_only", existing_type=sa.BOOLEAN(), nullable=False)
        bop.alter_column("async", existing_type=sa.BOOLEAN(), nullable=False)
        bop.alter_column(
            "cost",
            existing_type=postgresql.DOUBLE_PRECISION(precision=53),
            nullable=False,
        )
    op.drop_index("ix_database_URI", table_name="database")
    op.drop_index("ix_database_async", table_name="database")
    op.drop_index("ix_database_cost", table_name="database")
    op.drop_index("ix_database_description", table_name="database")
    op.drop_index("ix_database_id", table_name="database")
    op.drop_index("ix_database_read_only", table_name="database")
    with op.batch_alter_table("node") as bop:
        bop.alter_column("description", existing_type=sa.VARCHAR(), nullable=False)
    op.drop_index("ix_node_description", table_name="node")
    op.drop_index("ix_node_id", table_name="node")
    op.drop_index("ix_node_query", table_name="node")
    op.drop_index("ix_nodecolumns_column_id", table_name="nodecolumns")
    op.drop_index("ix_nodecolumns_node_id", table_name="nodecolumns")
    op.drop_index("ix_noderelationship_child_id", table_name="noderelationship")
    op.drop_index("ix_noderelationship_parent_id", table_name="noderelationship")
    with op.batch_alter_table("query") as bop:
        bop.alter_column("state", existing_type=sa.VARCHAR(), nullable=False)
        bop.alter_column(
            "progress",
            existing_type=postgresql.DOUBLE_PRECISION(precision=53),
            nullable=False,
        )
    op.drop_index("ix_query_catalog", table_name="query")
    op.drop_index("ix_query_database_id", table_name="query")
    op.drop_index("ix_query_executed_query", table_name="query")
    op.drop_index("ix_query_finished", table_name="query")
    op.drop_index("ix_query_progress", table_name="query")
    op.drop_index("ix_query_scheduled", table_name="query")
    op.drop_index("ix_query_schema_", table_name="query")
    op.drop_index("ix_query_started", table_name="query")
    op.drop_index("ix_query_state", table_name="query")
    op.drop_index("ix_query_submitted_query", table_name="query")
    with op.batch_alter_table("table") as bop:
        bop.alter_column(
            "cost",
            existing_type=postgresql.DOUBLE_PRECISION(precision=53),
            nullable=False,
        )
    op.drop_index("ix_table_catalog", table_name="table")
    op.drop_index("ix_table_cost", table_name="table")
    op.drop_index("ix_table_database_id", table_name="table")
    op.drop_index("ix_table_id", table_name="table")
    op.drop_index("ix_table_node_id", table_name="table")
    op.drop_index("ix_table_schema_", table_name="table")
    op.drop_index("ix_table_table", table_name="table")
    op.drop_index("ix_tablecolumns_column_id", table_name="tablecolumns")
    op.drop_index("ix_tablecolumns_table_id", table_name="tablecolumns")


def downgrade():
    op.create_index(
        "ix_tablecolumns_table_id",
        "tablecolumns",
        ["table_id"],
        unique=False,
    )
    op.create_index(
        "ix_tablecolumns_column_id",
        "tablecolumns",
        ["column_id"],
        unique=False,
    )
    op.create_index("ix_table_table", "table", ["table"], unique=False)
    op.create_index("ix_table_schema_", "table", ["schema_"], unique=False)
    op.create_index("ix_table_node_id", "table", ["node_id"], unique=False)
    op.create_index("ix_table_id", "table", ["id"], unique=False)
    op.create_index("ix_table_database_id", "table", ["database_id"], unique=False)
    op.create_index("ix_table_cost", "table", ["cost"], unique=False)
    op.create_index("ix_table_catalog", "table", ["catalog"], unique=False)
    with op.batch_alter_table("table") as bop:
        bop.alter_column(
            "cost",
            existing_type=postgresql.DOUBLE_PRECISION(precision=53),
            nullable=True,
        )
    op.create_index(
        "ix_query_submitted_query",
        "query",
        ["submitted_query"],
        unique=False,
    )
    op.create_index("ix_query_state", "query", ["state"], unique=False)
    op.create_index("ix_query_started", "query", ["started"], unique=False)
    op.create_index("ix_query_schema_", "query", ["schema_"], unique=False)
    op.create_index("ix_query_scheduled", "query", ["scheduled"], unique=False)
    op.create_index("ix_query_progress", "query", ["progress"], unique=False)
    op.create_index("ix_query_finished", "query", ["finished"], unique=False)
    op.create_index(
        "ix_query_executed_query",
        "query",
        ["executed_query"],
        unique=False,
    )
    op.create_index("ix_query_database_id", "query", ["database_id"], unique=False)
    op.create_index("ix_query_catalog", "query", ["catalog"], unique=False)
    with op.batch_alter_table("query") as bop:
        bop.alter_column(
            "progress",
            existing_type=postgresql.DOUBLE_PRECISION(precision=53),
            nullable=True,
        )
        bop.alter_column("state", existing_type=sa.VARCHAR(), nullable=True)
    op.create_index(
        "ix_noderelationship_parent_id",
        "noderelationship",
        ["parent_id"],
        unique=False,
    )
    op.create_index(
        "ix_noderelationship_child_id",
        "noderelationship",
        ["child_id"],
        unique=False,
    )
    op.create_index("ix_nodecolumns_node_id", "nodecolumns", ["node_id"], unique=False)
    op.create_index(
        "ix_nodecolumns_column_id",
        "nodecolumns",
        ["column_id"],
        unique=False,
    )
    op.create_index("ix_node_query", "node", ["query"], unique=False)
    op.create_index("ix_node_id", "node", ["id"], unique=False)
    op.create_index("ix_node_description", "node", ["description"], unique=False)
    with op.batch_alter_table("node") as bop:
        bop.alter_column("description", existing_type=sa.VARCHAR(), nullable=True)
    op.create_index("ix_database_read_only", "database", ["read_only"], unique=False)
    op.create_index("ix_database_id", "database", ["id"], unique=False)
    op.create_index(
        "ix_database_description",
        "database",
        ["description"],
        unique=False,
    )
    op.create_index("ix_database_cost", "database", ["cost"], unique=False)
    op.create_index("ix_database_async", "database", ["async"], unique=False)
    op.create_index("ix_database_URI", "database", ["URI"], unique=False)
    with op.batch_alter_table("database") as bop:
        bop.alter_column(
            "cost",
            existing_type=postgresql.DOUBLE_PRECISION(precision=53),
            nullable=True,
        )
        bop.alter_column("async", existing_type=sa.BOOLEAN(), nullable=True)
        bop.alter_column("read_only", existing_type=sa.BOOLEAN(), nullable=True)
        bop.alter_column(
            "description",
            existing_type=sa.VARCHAR(),
            nullable=True,
        )
    op.create_index("ix_column_name", "column", ["name"], unique=False)
    op.create_index("ix_column_id", "column", ["id"], unique=False)
    op.create_index("ix_column_dimension_id", "column", ["dimension_id"], unique=False)
    op.create_index(
        "ix_column_dimension_column",
        "column",
        ["dimension_column"],
        unique=False,
    )
