"""Initial migration

Revision ID: 5f31ff8814e7
Revises:
Create Date: 2022-04-30 19:39:20.164043+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlalchemy_utils
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "5f31ff8814e7"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "database",
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), nullable=True),
        sa.Column("description", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("URI", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("read_only", sa.Boolean(), nullable=True),
        sa.Column("async", sa.Boolean(), nullable=True),
        sa.Column("cost", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_index(op.f("ix_database_URI"), "database", ["URI"], unique=False)
    op.create_index(op.f("ix_database_async"), "database", ["async"], unique=False)
    op.create_index(op.f("ix_database_cost"), "database", ["cost"], unique=False)
    op.create_index(
        op.f("ix_database_description"),
        "database",
        ["description"],
        unique=False,
    )
    op.create_index(op.f("ix_database_id"), "database", ["id"], unique=False)
    op.create_index(
        op.f("ix_database_read_only"),
        "database",
        ["read_only"],
        unique=False,
    )
    op.create_table(
        "node",
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "type",
            sa.Enum("SOURCE", "TRANSFORM", "METRIC", "DIMENSION", name="nodetype"),
            nullable=True,
        ),
        sa.Column("id", sa.Integer(), nullable=True),
        sa.Column("description", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("expression", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_index(op.f("ix_node_description"), "node", ["description"], unique=False)
    op.create_index(op.f("ix_node_expression"), "node", ["expression"], unique=False)
    op.create_index(op.f("ix_node_id"), "node", ["id"], unique=False)
    op.create_table(
        "column",
        sa.Column(
            "type",
            sa.Enum(
                "BYTES",
                "STR",
                "FLOAT",
                "INT",
                "DECIMAL",
                "BOOL",
                "DATETIME",
                "DATE",
                "TIME",
                "TIMEDELTA",
                "LIST",
                "DICT",
                name="columntype",
            ),
            nullable=True,
        ),
        sa.Column("id", sa.Integer(), nullable=True),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("dimension_id", sa.Integer(), nullable=True),
        sa.Column(
            "dimension_column",
            sqlmodel.sql.sqltypes.AutoString(),
            nullable=True,
        ),
        sa.ForeignKeyConstraint(
            ["dimension_id"],
            ["node.id"],
            name="fk_column_dimension_id",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_column_dimension_column"),
        "column",
        ["dimension_column"],
        unique=False,
    )
    op.create_index(
        op.f("ix_column_dimension_id"),
        "column",
        ["dimension_id"],
        unique=False,
    )
    op.create_index(op.f("ix_column_id"), "column", ["id"], unique=False)
    op.create_index(op.f("ix_column_name"), "column", ["name"], unique=False)
    op.create_table(
        "noderelationship",
        sa.Column("parent_id", sa.Integer(), nullable=True),
        sa.Column("child_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["child_id"],
            ["node.id"],
            name="fk_noderelationship_child_id",
        ),
        sa.ForeignKeyConstraint(
            ["parent_id"],
            ["node.id"],
            name="fk_noderelationship_parent_id",
        ),
        sa.PrimaryKeyConstraint("parent_id", "child_id"),
    )
    op.create_index(
        op.f("ix_noderelationship_child_id"),
        "noderelationship",
        ["child_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_noderelationship_parent_id"),
        "noderelationship",
        ["parent_id"],
        unique=False,
    )
    op.create_table(
        "query",
        sa.Column("id", sqlalchemy_utils.types.uuid.UUIDType(), nullable=False),
        sa.Column("database_id", sa.Integer(), nullable=False),
        sa.Column("catalog", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("schema_", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column(
            "submitted_query",
            sqlmodel.sql.sqltypes.AutoString(),
            nullable=False,
        ),
        sa.Column("executed_query", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("scheduled", sa.DateTime(), nullable=True),
        sa.Column("started", sa.DateTime(), nullable=True),
        sa.Column("finished", sa.DateTime(), nullable=True),
        sa.Column("state", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("progress", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(
            ["database_id"],
            ["database.id"],
            name="fk_query_database_id",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_query_catalog"), "query", ["catalog"], unique=False)
    op.create_index(
        op.f("ix_query_database_id"),
        "query",
        ["database_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_query_executed_query"),
        "query",
        ["executed_query"],
        unique=False,
    )
    op.create_index(op.f("ix_query_finished"), "query", ["finished"], unique=False)
    op.create_index(op.f("ix_query_progress"), "query", ["progress"], unique=False)
    op.create_index(op.f("ix_query_scheduled"), "query", ["scheduled"], unique=False)
    op.create_index(op.f("ix_query_schema_"), "query", ["schema_"], unique=False)
    op.create_index(op.f("ix_query_started"), "query", ["started"], unique=False)
    op.create_index(op.f("ix_query_state"), "query", ["state"], unique=False)
    op.create_index(
        op.f("ix_query_submitted_query"),
        "query",
        ["submitted_query"],
        unique=False,
    )
    op.create_table(
        "table",
        sa.Column("id", sa.Integer(), nullable=True),
        sa.Column("node_id", sa.Integer(), nullable=False),
        sa.Column("database_id", sa.Integer(), nullable=False),
        sa.Column("catalog", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("schema_", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("table", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("cost", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(
            ["database_id"], ["database.id"], name="fk_table_database_id",
        ),
        sa.ForeignKeyConstraint(
            ["node_id"],
            ["node.id"],
            name="fk_table_node_id",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_table_catalog"), "table", ["catalog"], unique=False)
    op.create_index(op.f("ix_table_cost"), "table", ["cost"], unique=False)
    op.create_index(
        op.f("ix_table_database_id"),
        "table",
        ["database_id"],
        unique=False,
    )
    op.create_index(op.f("ix_table_id"), "table", ["id"], unique=False)
    op.create_index(op.f("ix_table_node_id"), "table", ["node_id"], unique=False)
    op.create_index(op.f("ix_table_schema_"), "table", ["schema_"], unique=False)
    op.create_index(op.f("ix_table_table"), "table", ["table"], unique=False)
    op.create_table(
        "nodecolumns",
        sa.Column("node_id", sa.Integer(), nullable=True),
        sa.Column("column_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["column_id"],
            ["column.id"],
            name="fk_nodecolumns_column_id",
        ),
        sa.ForeignKeyConstraint(
            ["node_id"],
            ["node.id"],
            name="fk_nodecolumns_node_id",
        ),
        sa.PrimaryKeyConstraint("node_id", "column_id"),
    )
    op.create_index(
        op.f("ix_nodecolumns_column_id"),
        "nodecolumns",
        ["column_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_nodecolumns_node_id"),
        "nodecolumns",
        ["node_id"],
        unique=False,
    )
    op.create_table(
        "tablecolumns",
        sa.Column("table_id", sa.Integer(), nullable=True),
        sa.Column("column_id", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(
            ["column_id"],
            ["column.id"],
            name="fk_tablecolumns_column_id",
        ),
        sa.ForeignKeyConstraint(
            ["table_id"],
            ["table.id"],
            name="fk_tablecolumns_table_id",
        ),
        sa.PrimaryKeyConstraint("table_id", "column_id"),
    )
    op.create_index(
        op.f("ix_tablecolumns_column_id"),
        "tablecolumns",
        ["column_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_tablecolumns_table_id"),
        "tablecolumns",
        ["table_id"],
        unique=False,
    )


def downgrade():
    op.drop_index(op.f("ix_tablecolumns_table_id"), table_name="tablecolumns")
    op.drop_index(op.f("ix_tablecolumns_column_id"), table_name="tablecolumns")
    op.drop_table("tablecolumns")
    op.drop_index(op.f("ix_nodecolumns_node_id"), table_name="nodecolumns")
    op.drop_index(op.f("ix_nodecolumns_column_id"), table_name="nodecolumns")
    op.drop_table("nodecolumns")
    op.drop_index(op.f("ix_table_table"), table_name="table")
    op.drop_index(op.f("ix_table_schema_"), table_name="table")
    op.drop_index(op.f("ix_table_node_id"), table_name="table")
    op.drop_index(op.f("ix_table_id"), table_name="table")
    op.drop_index(op.f("ix_table_database_id"), table_name="table")
    op.drop_index(op.f("ix_table_cost"), table_name="table")
    op.drop_index(op.f("ix_table_catalog"), table_name="table")
    op.drop_table("table")
    op.drop_index(op.f("ix_query_submitted_query"), table_name="query")
    op.drop_index(op.f("ix_query_state"), table_name="query")
    op.drop_index(op.f("ix_query_started"), table_name="query")
    op.drop_index(op.f("ix_query_schema_"), table_name="query")
    op.drop_index(op.f("ix_query_scheduled"), table_name="query")
    op.drop_index(op.f("ix_query_progress"), table_name="query")
    op.drop_index(op.f("ix_query_finished"), table_name="query")
    op.drop_index(op.f("ix_query_executed_query"), table_name="query")
    op.drop_index(op.f("ix_query_database_id"), table_name="query")
    op.drop_index(op.f("ix_query_catalog"), table_name="query")
    op.drop_table("query")
    op.drop_index(op.f("ix_noderelationship_parent_id"), table_name="noderelationship")
    op.drop_index(op.f("ix_noderelationship_child_id"), table_name="noderelationship")
    op.drop_table("noderelationship")
    op.drop_index(op.f("ix_column_name"), table_name="column")
    op.drop_index(op.f("ix_column_id"), table_name="column")
    op.drop_index(op.f("ix_column_dimension_id"), table_name="column")
    op.drop_index(op.f("ix_column_dimension_column"), table_name="column")
    op.drop_table("column")
    op.drop_index(op.f("ix_node_id"), table_name="node")
    op.drop_index(op.f("ix_node_expression"), table_name="node")
    op.drop_index(op.f("ix_node_description"), table_name="node")
    op.drop_table("node")
    op.drop_index(op.f("ix_database_read_only"), table_name="database")
    op.drop_index(op.f("ix_database_id"), table_name="database")
    op.drop_index(op.f("ix_database_description"), table_name="database")
    op.drop_index(op.f("ix_database_cost"), table_name="database")
    op.drop_index(op.f("ix_database_async"), table_name="database")
    op.drop_index(op.f("ix_database_URI"), table_name="database")
    op.drop_table("database")
