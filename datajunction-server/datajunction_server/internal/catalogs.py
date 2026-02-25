"""
Utilities for catalog introspection and source table discovery.
"""

from dataclasses import dataclass
from typing import List

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Inspector
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.catalog import Catalog
from datajunction_server.errors import DJException


@dataclass
class IntrospectedColumn:
    """Column information from catalog introspection."""

    name: str
    type: str


def sqlalchemy_type_to_dj_type(sqlalchemy_type_str: str) -> str:
    """
    Convert SQLAlchemy type string to DJ type string.

    Args:
        sqlalchemy_type_str: String representation of SQLAlchemy type

    Returns:
        DJ type string (e.g., 'string', 'int', 'bigint', etc.)
    """
    type_lower = sqlalchemy_type_str.lower()

    # Map SQLAlchemy types to DJ types
    if "varchar" in type_lower or "char" in type_lower or "text" in type_lower:
        if "longtext" in type_lower or "clob" in type_lower:
            return "longtext"
        return "string"
    elif "int" in type_lower:
        if "bigint" in type_lower:
            return "bigint"
        elif "smallint" in type_lower:
            return "smallint"
        elif "tinyint" in type_lower:
            return "tinyint"
        return "int"
    elif "float" in type_lower:
        return "float"
    elif "double" in type_lower or "real" in type_lower:
        return "double"
    elif "decimal" in type_lower or "numeric" in type_lower:
        return "decimal"
    elif "bool" in type_lower:
        return "boolean"
    elif "date" in type_lower:
        if "datetime" in type_lower or "timestamp" in type_lower:
            return "timestamp"
        return "date"
    elif "time" in type_lower:
        if "timestamp" in type_lower:
            return "timestamp"
        return "datetime"
    elif "array" in type_lower or "list" in type_lower:
        return "list"

    # Default to string for unknown types
    return "string"


async def introspect_table_schema(
    session: AsyncSession,
    catalog_name: str,
    schema_name: str,
    table_name: str,
) -> List[IntrospectedColumn]:
    """
    Introspect a table schema from a catalog using SQLAlchemy Inspector.

    This function connects to the catalog's engine and uses SQLAlchemy's
    Inspector to retrieve column metadata for the specified table.

    Args:
        session: Database session
        catalog_name: Name of the catalog containing the table
        schema_name: Schema name within the catalog
        table_name: Table name to introspect

    Returns:
        List of IntrospectedColumn objects with column names and types

    Raises:
        DJException: If catalog not found, has no engines, or introspection fails
    """
    # Get catalog from database
    catalog = await Catalog.get_by_name(session, catalog_name)
    if not catalog:
        raise DJException(
            message=f"Catalog `{catalog_name}` not found",
            http_status_code=404,
        )

    # Check if catalog has engines
    if not catalog.engines:
        raise DJException(
            message=(
                f"Catalog `{catalog_name}` has no engines configured. "
                f"Cannot introspect table `{schema_name}.{table_name}`."
            ),
            http_status_code=400,
        )

    # Use the first available engine for introspection
    engine_db = catalog.engines[0]

    if not engine_db.uri:
        raise DJException(
            message=(
                f"Engine `{engine_db.name}` for catalog `{catalog_name}` "
                f"has no URI configured. Cannot introspect table."
            ),
            http_status_code=400,
        )

    try:
        # Create a SQLAlchemy engine from the URI
        # Note: Using sync create_engine for simplicity with Inspector
        engine = create_engine(engine_db.uri, pool_pre_ping=True)

        # Create Inspector for schema introspection
        inspector: Inspector = inspect(engine)

        # Get column information
        columns = inspector.get_columns(table_name, schema=schema_name)

        if not columns:
            raise DJException(
                message=(
                    f"Table `{schema_name}.{table_name}` not found in catalog "
                    f"`{catalog_name}` or has no columns"
                ),
                http_status_code=404,
            )

        # Convert to our column format
        introspected_columns = []
        for col in columns:
            col_type_str = str(col["type"])
            dj_type = sqlalchemy_type_to_dj_type(col_type_str)
            introspected_columns.append(
                IntrospectedColumn(
                    name=col["name"],
                    type=dj_type,
                ),
            )

        # Clean up the engine
        engine.dispose()

        return introspected_columns

    except Exception as exc:
        if isinstance(exc, DJException):
            raise
        raise DJException(
            message=(
                f"Failed to introspect table `{schema_name}.{table_name}` "
                f"from catalog `{catalog_name}`: {str(exc)}"
            ),
            http_status_code=500,
        ) from exc


def parse_source_node_name(node_name: str) -> tuple[str, str, str]:
    """
    Parse a source node name to extract catalog, schema, and table.

    Expected formats:
        catalog.schema.table -> (catalog, schema, table)
        source.catalog.schema.table -> (catalog, schema, table)

    Args:
        node_name: Node name to parse

    Returns:
        Tuple of (catalog_name, schema_name, table_name)

    Raises:
        DJException: If node name doesn't match expected format
    """
    parts = node_name.split(".")

    # Strip "source" prefix if present
    if parts[0] == "source":
        parts = parts[1:]

    if len(parts) != 3:
        raise DJException(
            message=(
                f"Invalid source node name: `{node_name}`. "
                f"Expected format: catalog.schema.table or source.catalog.schema.table"
            ),
            http_status_code=400,
        )

    return parts[0], parts[1], parts[2]
