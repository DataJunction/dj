"""Initial migration

Revision ID: b9f370ef912a
Revises:
Create Date: 2023-02-10 16:21:58.012468+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlalchemy_utils
import sqlmodel

from alembic import op

# revision identifiers, used by Alembic.
revision = "b9f370ef912a"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "availabilitystate",
        sa.Column("max_partition", sa.JSON(), nullable=True),
        sa.Column("min_partition", sa.JSON(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("catalog", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("schema_", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("table", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("valid_through_ts", sa.Integer(), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_availabilitystate")),
    )
    op.create_table(
        "catalog",
        sa.Column("uuid", sqlalchemy_utils.types.uuid.UUIDType(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("extra_params", sa.JSON(), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_catalog")),
    )
    op.create_table(
        "database",
        sa.Column("uuid", sqlalchemy_utils.types.uuid.UUIDType(), nullable=True),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("extra_params", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("description", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("URI", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("read_only", sa.Boolean(), nullable=False),
        sa.Column("async", sa.Boolean(), nullable=False),
        sa.Column("cost", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_database")),
        sa.UniqueConstraint("name", name=op.f("uq_database_name")),
    )
    op.create_table(
        "engine",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("version", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("uri", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_engine")),
    )
    op.create_table(
        "missingparent",
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_missingparent")),
    )
    op.create_table(
        "node",
        sa.Column("name", sa.String(), nullable=True),
        sa.Column(
            "type",
            sa.Enum("SOURCE", "TRANSFORM", "METRIC", "DIMENSION", name="nodetype"),
            nullable=True,
        ),
        sa.Column("display_name", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "current_version",
            sqlmodel.sql.sqltypes.AutoString(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_node")),
        sa.UniqueConstraint("name", name=op.f("uq_node_name")),
    )
    op.create_table(
        "catalogengines",
        sa.Column("catalog_id", sa.Integer(), nullable=False),
        sa.Column("engine_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["catalog_id"],
            ["catalog.id"],
            name=op.f("fk_catalogengines_catalog_id_catalog"),
        ),
        sa.ForeignKeyConstraint(
            ["engine_id"],
            ["engine.id"],
            name=op.f("fk_catalogengines_engine_id_engine"),
        ),
        sa.PrimaryKeyConstraint(
            "catalog_id",
            "engine_id",
            name=op.f("pk_catalogengines"),
        ),
    )
    op.create_table(
        "column",
        sa.Column(
            "type",
            sa.String(),
            nullable=False,
        ),
        sa.Column("id", sa.Integer(), nullable=False),
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
            name=op.f("fk_column_dimension_id_node"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_column")),
    )
    op.create_table(
        "noderevision",
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("display_name", sa.String(), nullable=True),
        sa.Column(
            "type",
            sa.Enum(
                "SOURCE",
                "TRANSFORM",
                "METRIC",
                "DIMENSION",
                name="nodetype",
                create=False,
            ),
            nullable=True,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("description", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("query", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("mode", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("version", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("node_id", sa.Integer(), nullable=True),
        sa.Column("status", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.ForeignKeyConstraint(
            ["node_id"],
            ["node.id"],
            name=op.f("fk_noderevision_node_id_node"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_noderevision")),
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
        sa.Column("state", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("progress", sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(
            ["database_id"],
            ["database.id"],
            name=op.f("fk_query_database_id_database"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_query")),
    )
    op.create_table(
        "table",
        sa.Column("schema_", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("table", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("cost", sa.Float(), nullable=False),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("catalog_id", sa.Integer(), nullable=True),
        sa.Column("database_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["catalog_id"],
            ["catalog.id"],
            name=op.f("fk_table_catalog_id_catalog"),
        ),
        sa.ForeignKeyConstraint(
            ["database_id"],
            ["database.id"],
            name=op.f("fk_table_database_id_database"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_table")),
    )
    op.create_table(
        "nodeavailabilitystate",
        sa.Column("availability_id", sa.Integer(), nullable=False),
        sa.Column("node_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["availability_id"],
            ["availabilitystate.id"],
            name=op.f("fk_nodeavailabilitystate_availability_id_availabilitystate"),
        ),
        sa.ForeignKeyConstraint(
            ["node_id"],
            ["noderevision.id"],
            name=op.f("fk_nodeavailabilitystate_node_id_noderevision"),
        ),
        sa.PrimaryKeyConstraint(
            "availability_id",
            "node_id",
            name=op.f("pk_nodeavailabilitystate"),
        ),
    )
    op.create_table(
        "nodecolumns",
        sa.Column("node_id", sa.Integer(), nullable=False),
        sa.Column("column_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["column_id"],
            ["column.id"],
            name=op.f("fk_nodecolumns_column_id_column"),
        ),
        sa.ForeignKeyConstraint(
            ["node_id"],
            ["noderevision.id"],
            name=op.f("fk_nodecolumns_node_id_noderevision"),
        ),
        sa.PrimaryKeyConstraint("node_id", "column_id", name=op.f("pk_nodecolumns")),
    )
    op.create_table(
        "nodemissingparents",
        sa.Column("missing_parent_id", sa.Integer(), nullable=False),
        sa.Column("referencing_node_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["missing_parent_id"],
            ["missingparent.id"],
            name=op.f("fk_nodemissingparents_missing_parent_id_missingparent"),
        ),
        sa.ForeignKeyConstraint(
            ["referencing_node_id"],
            ["noderevision.id"],
            name=op.f("fk_nodemissingparents_referencing_node_id_noderevision"),
        ),
        sa.PrimaryKeyConstraint(
            "missing_parent_id",
            "referencing_node_id",
            name=op.f("pk_nodemissingparents"),
        ),
    )
    op.create_table(
        "noderelationship",
        sa.Column("parent_id", sa.Integer(), nullable=False),
        sa.Column("parent_version", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("child_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["child_id"],
            ["noderevision.id"],
            name=op.f("fk_noderelationship_child_id_noderevision"),
        ),
        sa.ForeignKeyConstraint(
            ["parent_id"],
            ["node.id"],
            name=op.f("fk_noderelationship_parent_id_node"),
        ),
        sa.PrimaryKeyConstraint(
            "parent_id",
            "child_id",
            name=op.f("pk_noderelationship"),
        ),
    )
    op.create_table(
        "tablecolumns",
        sa.Column("table_id", sa.Integer(), nullable=False),
        sa.Column("column_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["column_id"],
            ["column.id"],
            name=op.f("fk_tablecolumns_column_id_column"),
        ),
        sa.ForeignKeyConstraint(
            ["table_id"],
            ["table.id"],
            name=op.f("fk_tablecolumns_table_id_table"),
        ),
        sa.PrimaryKeyConstraint("table_id", "column_id", name=op.f("pk_tablecolumns")),
    )
    op.create_table(
        "tablenoderevision",
        sa.Column("table_id", sa.Integer(), nullable=False),
        sa.Column("node_revision_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["node_revision_id"],
            ["noderevision.id"],
            name=op.f("fk_tablenoderevision_node_revision_id_noderevision"),
        ),
        sa.ForeignKeyConstraint(
            ["table_id"],
            ["table.id"],
            name=op.f("fk_tablenoderevision_table_id_table"),
        ),
        sa.PrimaryKeyConstraint(
            "table_id",
            "node_revision_id",
            name=op.f("pk_tablenoderevision"),
        ),
    )


def downgrade():
    op.drop_table("tablenoderevision")
    op.drop_table("tablecolumns")
    op.drop_table("noderelationship")
    op.drop_table("nodemissingparents")
    op.drop_table("nodecolumns")
    op.drop_table("nodeavailabilitystate")
    op.drop_table("table")
    op.drop_table("query")
    op.drop_table("noderevision")
    op.drop_table("column")
    op.drop_table("catalogengines")
    op.drop_table("node")
    op.drop_table("missingparent")
    op.drop_table("engine")
    op.drop_table("database")
    op.drop_table("catalog")
    op.drop_table("availabilitystate")
