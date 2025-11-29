"""Snowflake query client using direct snowflake-connector-python."""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from datajunction_server.database.column import Column
from datajunction_server.errors import (
    DJDoesNotExistException,
    DJQueryServiceClientException,
)
from datajunction_server.query_clients.base import BaseQueryServiceClient
from datajunction_server.sql.parsing.types import ColumnType

if TYPE_CHECKING:
    from datajunction_server.database.engine import Engine

try:  # pragma: no cover
    import snowflake.connector
    from snowflake.connector import DictCursor
    from snowflake.connector import DatabaseError as SnowflakeDatabaseError

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
        password: Optional[str] = None,
        warehouse: str = "COMPUTE_WH",
        database: str = "SNOWFLAKE",
        schema: str = "PUBLIC",
        role: Optional[str] = None,
        authenticator: str = "snowflake",
        private_key_path: Optional[str] = None,
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

        self.connection_params: Dict[str, Any] = {
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

    def _get_connection(self):
        """Get a Snowflake connection."""
        return snowflake.connector.connect(**self.connection_params)

    def _get_database_from_engine(self, engine, fallback_catalog: str) -> str:
        """
        Extract the actual Snowflake database name from the engine URI.

        Args:
            engine: The DJ Engine object containing connection info
            fallback_catalog: Fallback catalog name if engine parsing fails

        Returns:
            The actual Snowflake database name to use
        """
        if not engine or not engine.uri:
            # No engine provided, use the configured default database
            return self.connection_params.get("database", fallback_catalog)

        try:
            # Parse the engine URI to extract database name
            # Snowflake URIs typically look like: snowflake://user:pass@account/database?warehouse=wh
            from urllib.parse import urlparse

            parsed = urlparse(engine.uri)

            if parsed.path and len(parsed.path) > 1:  # path starts with '/'
                database_name = parsed.path.lstrip("/")
                # Handle case where path might have additional segments
                database_name = database_name.split("/")[0]
                if database_name:
                    return database_name

            # If we can't parse the database from URI, check query parameters
            if parsed.query:
                from urllib.parse import parse_qs

                query_params = parse_qs(parsed.query)
                if "database" in query_params and query_params["database"]:
                    return query_params["database"][0]

        except Exception as e:  # pragma: no cover
            _logger.warning(
                f"Failed to parse database from engine URI {engine.uri}: {e}. "
                f"Using fallback: {fallback_catalog}",
            )

        # Fall back to the configured database or catalog name
        return self.connection_params.get("database", fallback_catalog)

    def get_columns_for_table(
        self,
        catalog: str,
        schema: str,
        table: str,
        request_headers: Optional[Dict[str, str]] = None,
        engine: Optional["Engine"] = None,
    ) -> List[Column]:
        """
        Retrieves columns for a table from Snowflake information schema.
        """
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
                    actual_database = self._get_database_from_engine(engine, catalog)
                    raise DJDoesNotExistException(
                        message=f"Table not found: {actual_database}.{schema}.{table} "
                        f"(DJ catalog: {catalog})",
                    )
                raise DJQueryServiceClientException(
                    message=f"Error retrieving columns from Snowflake: {str(e)}",
                )
            _logger.exception(
                "Unexpected error in get_columns_for_table",
            )  # pragma: no cover
            raise DJQueryServiceClientException(
                message=f"Unexpected error retrieving columns: {str(e)}",
            )
        finally:  # pragma: no cover
            if "conn" in locals():
                conn.close()

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
            TimeType,
            TimestampType,
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
            _logger.error(f"Snowflake connection test failed: {str(e)}")
            return False
