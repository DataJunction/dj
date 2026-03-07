"""BigQuery query client using google-cloud-bigquery."""

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
    from google.cloud import bigquery
    from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
    from google.oauth2 import service_account

    BIGQUERY_AVAILABLE = True
except ImportError:  # pragma: no cover
    bigquery = None
    QueryJobConfig = None
    ScalarQueryParameter = None
    service_account = None
    BIGQUERY_AVAILABLE = False

_logger = logging.getLogger(__name__)


class BigQueryClient(BaseQueryServiceClient):
    """
    BigQuery query client using google-cloud-bigquery.

    This client connects directly to BigQuery without requiring an external query service.
    It implements table introspection via INFORMATION_SCHEMA.

    In DJ's terminology:
      - catalog  = GCP project ID
      - schema   = BigQuery dataset
      - table    = table name
    """

    def __init__(
        self,
        project: str,
        credentials_path: Optional[str] = None,
        credentials_info: Optional[Dict[str, Any]] = None,
        location: Optional[str] = None,
        **connection_kwargs,
    ):
        """
        Initialize the BigQuery client.

        Args:
            project: GCP project ID
            credentials_path: Path to a service account JSON key file
            credentials_info: Service account credentials as a dictionary
            location: Default BigQuery location (e.g. 'US', 'EU')
            **connection_kwargs: Additional keyword arguments passed to bigquery.Client
        """
        if not BIGQUERY_AVAILABLE:
            raise ImportError(
                "google-cloud-bigquery is required for BigQueryClient. "
                "Install with: pip install 'datajunction-server[bigquery]' "
                "or pip install google-cloud-bigquery",
            )

        self.project = project
        self.location = location
        self._credentials_path = credentials_path
        self._credentials_info = credentials_info
        self._connection_kwargs = connection_kwargs

    def _get_client(self):
        """Return a configured google.cloud.bigquery.Client instance."""
        credentials = None

        if self._credentials_info:
            credentials = service_account.Credentials.from_service_account_info(
                self._credentials_info,
            )
        elif self._credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                self._credentials_path,
            )

        return bigquery.Client(
            project=self.project,
            credentials=credentials,
            location=self.location,
            **self._connection_kwargs,
        )

    def get_columns_for_table(
        self,
        catalog: str,
        schema: str,
        table: str,
        request_headers: Optional[Dict[str, str]] = None,
        engine: Optional["Engine"] = None,
    ) -> List[Column]:
        """
        Retrieve columns for a BigQuery table via INFORMATION_SCHEMA.

        Args:
            catalog: GCP project ID (may be overridden by engine URI)
            schema: BigQuery dataset name
            table: Table name
            request_headers: Unused (kept for interface compatibility)
            engine: Optional DJ engine (URI not used for BigQuery)

        Returns:
            List of Column objects
        """
        project = self.project
        try:
            client = self._get_client()

            query = f"""
                SELECT
                    column_name,
                    data_type,
                    ordinal_position
                FROM `{project}.{schema}.INFORMATION_SCHEMA.COLUMNS`
                WHERE table_name = @table_name
                ORDER BY ordinal_position
            """

            job_config = QueryJobConfig(
                query_parameters=[
                    ScalarQueryParameter("table_name", "STRING", table),
                ],
            )

            results = client.query(query, job_config=job_config).result()
            rows = list(results)

            if not rows:
                raise DJDoesNotExistException(
                    message=f"No columns found for table {project}.{schema}.{table}",
                )

            return [
                Column(
                    name=row.column_name,
                    type=self._map_bigquery_type_to_dj(row.data_type),
                    order=row.ordinal_position - 1,  # Convert to 0-based index
                )
                for row in rows
            ]

        except DJDoesNotExistException:
            raise
        except Exception as e:
            _logger.exception("Error retrieving columns from BigQuery")
            raise DJQueryServiceClientException(
                message=f"Error retrieving columns from BigQuery: {str(e)}",
            ) from e

    def _map_bigquery_type_to_dj(self, bq_type: str) -> ColumnType:
        """
        Map a BigQuery data type string to a DJ ColumnType.

        Args:
            bq_type: BigQuery data type string (e.g. 'INT64', 'STRING')

        Returns:
            Corresponding ColumnType instance
        """
        from datajunction_server.sql.parsing.types import (
            BigIntType,
            BooleanType,
            DateType,
            DecimalType,
            FloatType,
            StringType,
            TimeType,
            TimestampType,
        )

        # Strip parameterized suffixes like NUMERIC(10, 2) → NUMERIC
        base_type = bq_type.upper().split("(")[0].strip()

        type_mapping = {
            # Integer types
            "INT64": BigIntType(),
            "INT": BigIntType(),
            "INTEGER": BigIntType(),
            "BIGINT": BigIntType(),
            "SMALLINT": BigIntType(),
            "TINYINT": BigIntType(),
            "BYTEINT": BigIntType(),
            # Float types
            "FLOAT64": FloatType(),
            "FLOAT": FloatType(),
            # Decimal/numeric types
            "NUMERIC": DecimalType(38, 9),
            "DECIMAL": DecimalType(38, 9),
            # DJ caps DecimalType precision at 38, so BIGNUMERIC uses max values
            "BIGNUMERIC": DecimalType(38, 38),
            "BIGDECIMAL": DecimalType(38, 38),
            # Boolean
            "BOOL": BooleanType(),
            "BOOLEAN": BooleanType(),
            # String / bytes
            "STRING": StringType(),
            "BYTES": StringType(),
            # Date / time
            "DATE": DateType(),
            "TIME": TimeType(),
            "DATETIME": TimestampType(),
            "TIMESTAMP": TimestampType(),
            # Complex / semi-structured types — map to string
            "ARRAY": StringType(),
            "STRUCT": StringType(),
            "RECORD": StringType(),
            "GEOGRAPHY": StringType(),
            "JSON": StringType(),
            "INTERVAL": StringType(),
        }

        return type_mapping.get(base_type, StringType())  # type: ignore

    def test_connection(self) -> bool:
        """
        Test the BigQuery connection by running a lightweight query.

        Returns:
            True if successful, False otherwise
        """
        try:
            client = self._get_client()
            list(client.query("SELECT 1").result())
            return True
        except Exception as e:  # pragma: no cover
            _logger.error("BigQuery connection test failed: %s", str(e))
            return False
