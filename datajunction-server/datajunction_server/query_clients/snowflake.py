"""Snowflake query client using direct snowflake-connector-python."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from datajunction_server.database.column import Column
from datajunction_server.errors import (
    DJDoesNotExistException,
    DJQueryServiceClientException,
)
from datajunction_server.query_clients.base import BaseQueryServiceClient
from datajunction_server.query_clients.catalog import (
    catalog_from_engine_uri,
    replace_catalog_alias,
)
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.query import (
    ColumnMetadata,
    QueryCreate,
    QueryWithResults,
)
from datajunction_server.sql.parsing.types import ColumnType

if TYPE_CHECKING:
    from datajunction_server.database.engine import Engine

try:  # pragma: no cover
    import snowflake.connector
    from snowflake.connector import DatabaseError as SnowflakeDatabaseError
    from snowflake.connector import DictCursor

    SNOWFLAKE_AVAILABLE = True
except ImportError:  # pragma: no cover
    snowflake = None
    DictCursor = None
    SnowflakeDatabaseError = None
    SNOWFLAKE_AVAILABLE = False

_logger = logging.getLogger(__name__)


class SnowflakeClient(BaseQueryServiceClient):
    """
    Snowflake query client using direct snowflake-connector-python.

    This client connects directly to Snowflake without requiring an external query service.
    It implements the essential methods for table introspection and querying.
    """

    def __init__(
        self,
        account: str,
        user: str,
        password: str | None = None,
        warehouse: str = "COMPUTE_WH",
        database: str = "SNOWFLAKE",
        schema: str = "PUBLIC",
        role: str | None = None,
        authenticator: str = "snowflake",
        private_key_path: str | None = None,
        **connection_kwargs,
    ):
        """
        Initialize the Snowflake client.

        Args:
            account: Snowflake account identifier
            user: Username for authentication
            password: Password for authentication (optional if using key-based auth)
            warehouse: Default warehouse to use
            database: Default database to use
            schema: Default schema to use
            role: Role to assume (optional)
            authenticator: Authentication method ('snowflake', 'oauth', etc.)
            private_key_path: Path to private key file for key-based auth
            **connection_kwargs: Additional connection parameters
        """
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError(
                "snowflake-connector-python is required for SnowflakeClient. "
                "Install with: pip install 'datajunction-server[snowflake]' "
                "or pip install snowflake-connector-python",
            )

        self.connection_params: dict[str, Any] = {
            "account": account,
            "user": user,
            "warehouse": warehouse,
            "database": database,
            "schema": schema,
            "authenticator": authenticator,
            **connection_kwargs,
        }

        if password:
            self.connection_params["password"] = password
        if role:
            self.connection_params["role"] = role
        if private_key_path:
            # Load private key for key-based authentication
            with open(private_key_path, "rb") as f:
                private_key_bytes = f.read()
            self.connection_params["private_key"] = private_key_bytes

    def _get_connection(self, database: str | None = None):
        """Get a Snowflake connection."""
        params = {**self.connection_params}
        if database:
            params["database"] = database
        return snowflake.connector.connect(**params)

    async def submit_query(
        self,
        query_create: QueryCreate,
        request_headers: dict[str, str] | None = None,
    ) -> QueryWithResults:
        """Execute SQL with the configured Snowflake connection."""

        def _fetch() -> tuple[list[ColumnMetadata], list[tuple[Any, ...]], str]:
            database = catalog_from_engine_uri(
                query_create.engine_uri,
                Dialect.SNOWFLAKE,
            ) or self.connection_params.get("database", query_create.catalog_name)
            sql = replace_catalog_alias(
                query_create.submitted_query,
                query_create.catalog_name,
                database,
                Dialect.SNOWFLAKE,
            )
            conn = self._get_connection(database=database)
            try:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    columns = [
                        ColumnMetadata(name=col.name, type=str(col.type_code))
                        for col in cursor.description or []
                    ]
                    return columns, [tuple(row) for row in cursor.fetchall()], sql
            finally:
                conn.close()

        try:
            columns, rows, executed_sql = await asyncio.to_thread(_fetch)
        except Exception as exc:
            _logger.exception("Snowflake query failed")
            raise DJQueryServiceClientException(
                message="Snowflake query failed.",
            ) from exc
        return self._finished_query(query_create, columns, rows, executed_sql)

    def _get_database_from_engine(self, engine, fallback_catalog: str) -> str:
        """
        Extract the actual Snowflake database name from the engine URI.

        Args:
            engine: The DJ Engine object containing connection info
            fallback_catalog: Fallback catalog name if engine parsing fails

        Returns:
            The actual Snowflake database name to use
        """
        return catalog_from_engine_uri(
            engine.uri if engine else None,
            Dialect.SNOWFLAKE,
        ) or self.connection_params.get("database", fallback_catalog)

    async def get_columns_for_table(
        self,
        catalog: str,
        schema: str,
        table: str,
        request_headers: dict[str, str] | None = None,
        engine: Engine | None = None,
    ) -> list[Column]:
        """
        Retrieves columns for a table from Snowflake information schema.

        The snowflake-connector-python driver is sync, so we run the query
        in a threadpool to avoid blocking the event loop.
        """

        def _fetch() -> list[Column]:
            try:
                conn = self._get_connection()

                # Extract actual Snowflake database name from engine URI if provided
                actual_database = self._get_database_from_engine(engine, catalog)

                with conn.cursor(DictCursor) as cursor:
                    # Use Snowflake's INFORMATION_SCHEMA to get column information
                    query = """
                    SELECT
                        column_name,
                        data_type,
                        is_nullable,
                        ordinal_position
                    FROM information_schema.columns
                    WHERE table_catalog = %s
                        AND table_schema = %s
                        AND table_name = %s
                    ORDER BY ordinal_position
                    """

                    cursor.execute(
                        query,
                        (actual_database.upper(), schema.upper(), table.upper()),
                    )
                    rows = cursor.fetchall()

                    if not rows:
                        raise DJDoesNotExistException(
                            message=f"No columns found for table {actual_database}.{schema}.{table} "
                            f"(DJ catalog: {catalog})",
                        )

                    columns = []
                    for row in rows:
                        column_type = self._map_snowflake_type_to_dj(row["DATA_TYPE"])
                        columns.append(
                            Column(
                                name=row["COLUMN_NAME"],
                                type=column_type,
                                order=row["ORDINAL_POSITION"]
                                - 1,  # Convert to 0-based index
                            ),
                        )

                    return columns

            except DJDoesNotExistException:
                # Re-raise DJDoesNotExistException as-is
                raise
            except Exception as e:  # pragma: no cover
                # Check if it's a Snowflake DatabaseError (only if snowflake is available)
                if (
                    SNOWFLAKE_AVAILABLE
                    and SnowflakeDatabaseError
                    and isinstance(e, SnowflakeDatabaseError)
                ):
                    if "does not exist" in str(e).lower():
                        actual_database = self._get_database_from_engine(
                            engine,
                            catalog,
                        )
                        raise DJDoesNotExistException(
                            message=f"Table not found: {actual_database}.{schema}.{table} "
                            f"(DJ catalog: {catalog})",
                        )
                    raise DJQueryServiceClientException(
                        message=f"Error retrieving columns from Snowflake: {e!s}",
                    )
                _logger.exception(
                    "Unexpected error in get_columns_for_table",
                )  # pragma: no cover
                raise DJQueryServiceClientException(
                    message=f"Unexpected error retrieving columns: {e!s}",
                )
            finally:  # pragma: no cover
                if "conn" in locals():
                    conn.close()

        return await asyncio.to_thread(_fetch)

    def _map_snowflake_type_to_dj(self, snowflake_type: str) -> ColumnType:
        """
        Map Snowflake data types to DJ ColumnType.

        Args:
            snowflake_type: Snowflake data type string

        Returns:
            Corresponding ColumnType instance
        """
        from datajunction_server.sql.parsing.types import (
            BigIntType,
            BooleanType,
            DateType,
            DecimalType,
            DoubleType,
            FloatType,
            IntegerType,
            StringType,
            TimestampType,
            TimeType,
        )

        snowflake_type = snowflake_type.upper()

        # Check if it's a type with parameters
        if "(" in snowflake_type and snowflake_type.split("(")[0] in (
            "NUMBER",
            "DECIMAL",
            "NUMERIC",
        ):
            base_type = snowflake_type.split("(")[0]
            # For snowflake number/decimal/numeric types we have to extract precision and scale
            try:
                params = snowflake_type.split("(")[1].rstrip(")").split(",")
                precision = int(params[0].strip())
                scale = int(params[1].strip()) if len(params) > 1 else 0
                return DecimalType(precision, scale)
            except (ValueError, IndexError):
                pass  # Fall back to default decimal
        else:  # pragma: no cover
            base_type = snowflake_type

        type_mapping = {
            "NUMBER": DecimalType(38, 0),
            "DECIMAL": DecimalType(38, 0),
            "NUMERIC": DecimalType(38, 0),
            "INT": IntegerType(),
            "INTEGER": IntegerType(),
            "BIGINT": BigIntType(),
            "SMALLINT": IntegerType(),
            "TINYINT": IntegerType(),
            "BYTEINT": IntegerType(),
            "FLOAT": FloatType(),
            "FLOAT4": FloatType(),
            "FLOAT8": DoubleType(),
            "DOUBLE": DoubleType(),
            "DOUBLE PRECISION": DoubleType(),
            "REAL": FloatType(),
            "VARCHAR": StringType(),
            "CHAR": StringType(),
            "CHARACTER": StringType(),
            "STRING": StringType(),
            "TEXT": StringType(),
            "BINARY": StringType(),
            "VARBINARY": StringType(),
            "DATE": DateType(),
            "DATETIME": TimestampType(),
            "TIME": TimeType(),
            "TIMESTAMP": TimestampType(),
            "TIMESTAMP_LTZ": TimestampType(),
            "TIMESTAMP_NTZ": TimestampType(),
            "TIMESTAMP_TZ": TimestampType(),
            "BOOLEAN": BooleanType(),
            "VARIANT": StringType(),  # JSON-like type
            "OBJECT": StringType(),  # Object type
            "ARRAY": StringType(),  # Array type
            "GEOGRAPHY": StringType(),
            "GEOMETRY": StringType(),
        }

        return type_mapping.get(base_type, StringType())  # type: ignore

    def test_connection(self) -> bool:
        """
        Test the Snowflake connection.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            conn.close()
            return True
        except Exception as e:  # pragma: no cover
            _logger.error(f"Snowflake connection test failed: {e!s}")
            return False
