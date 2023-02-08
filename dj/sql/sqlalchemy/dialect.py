"""
SQLAlchemy dialect.
"""
# pylint: disable=abstract-method, unused-argument

from types import ModuleType
from typing import Any, Dict, List, Optional, Tuple, TypedDict

import requests
import sqlalchemy.types
from sqlalchemy.engine.base import Connection as SqlaConnection
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.engine.url import URL as SqlaURL
from sqlalchemy.sql import compiler
from sqlalchemy.sql.visitors import VisitableType
from yarl import URL

from dj.constants import GET_COLUMNS_TIMEOUT
from dj.sql import dbapi
from dj.sql.dbapi.connection import Connection
from dj.typing import ColumnType


class SQLAlchemyColumn(TypedDict):
    """
    A custom type for a SQLAlchemy column.
    """

    name: str
    type: VisitableType
    nullable: bool
    default: Optional[str]


def get_sqla_type(type_: ColumnType) -> VisitableType:
    """
    Convert from DJ to SQLA type.
    """
    type_map = {
        ColumnType.BYTES: sqlalchemy.types.BINARY,
        ColumnType.STR: sqlalchemy.types.TEXT,
        ColumnType.FLOAT: sqlalchemy.types.FLOAT,
        ColumnType.INT: sqlalchemy.types.INT,
        ColumnType.DECIMAL: sqlalchemy.types.DECIMAL,
        ColumnType.BOOL: sqlalchemy.types.BOOLEAN,
        ColumnType.TIMESTAMP: sqlalchemy.types.TIMESTAMP,
        ColumnType.DATE: sqlalchemy.types.DATE,
        ColumnType.TIME: sqlalchemy.types.TIME,
        # imperfect matches
        ColumnType.TIMEDELTA: sqlalchemy.types.TEXT,
        ColumnType.ARRAY: sqlalchemy.types.ARRAY,
        ColumnType.MAP: sqlalchemy.types.JSON,
    }
    return type_map[type_]()


class DJDialect(DefaultDialect):
    """
    A SQLAlchemy dialect for DJ.
    """

    name = "dj"
    driver = "rest"

    statement_compiler = compiler.SQLCompiler
    ddl_compiler = compiler.DDLCompiler
    type_compiler = compiler.GenericTypeCompiler
    preparer = compiler.IdentifierPreparer

    supports_alter = False
    supports_comments = True
    inline_comments = True
    supports_statement_cache = True

    supports_schemas = False
    supports_views = False
    postfetch_lastrowid = False

    supports_native_boolean = True

    isolation_level = "AUTOCOMMIT"

    default_paramstyle = "pyformat"

    supports_is_distinct_from = False

    @classmethod
    def dbapi(cls) -> ModuleType:  # pylint: disable=method-hidden
        """
        Return the DB API module.
        """
        return dbapi

    def create_connect_args(
        self,
        url: SqlaURL,
    ) -> Tuple[Tuple[str, int], Dict[str, Any]]:
        scheme = url.query.get("scheme", "http")

        args = url.translate_connect_args()
        database = args["database"]
        if "/" in database:
            path, database = database.rsplit("/", 1)
        else:
            path = ""
        database_id = int(database)
        base_url = URL.build(
            scheme=scheme,
            host=args["host"],
            port=args["port"],
            path="/" + path,
        )

        return (base_url, database_id), {}

    def do_ping(self, dbapi_connection: Connection) -> bool:
        """
        Is the service up?
        """
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute("SELECT 1")
        except Exception:  # pylint: disable=broad-except
            return False

        return True

    def has_table(
        self,
        connection: SqlaConnection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> bool:
        """
        Return if a given table exists.
        """
        return table_name == "metrics"

    def get_table_names(
        self,
        connection: SqlaConnection,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List[str]:
        """
        Return a list of table names.
        """
        return ["metrics"]

    def get_columns(
        self,
        connection: SqlaConnection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List[SQLAlchemyColumn]:
        """
        Return information about columns.

        For DJ this means all metric dimensions.
        """
        if table_name != "metrics":
            return []

        # extract base URL from the DB API connection
        base_url = connection.engine.connect().connection.base_url

        response = requests.get(
            base_url / "metrics/",
            timeout=GET_COLUMNS_TIMEOUT.total_seconds(),
        )
        payload = response.json()
        dimensions = {
            dimension for metric in payload for dimension in metric["dimensions"]
        }

        response = requests.get(
            base_url / "nodes/",
            timeout=GET_COLUMNS_TIMEOUT.total_seconds(),
        )
        payload = response.json()
        columns: Dict[str, SQLAlchemyColumn] = {}
        for node in payload:
            for column in node["columns"]:
                name = node["name"] + "." + column["name"]
                if name in dimensions and name not in columns:
                    columns[name] = {
                        "name": name,
                        "type": get_sqla_type(ColumnType(column["type"])),
                        "nullable": True,
                        "default": None,
                    }

        return list(columns.values())

    def do_rollback(self, dbapi_connection: Connection) -> None:
        """
        DJ doesn't support rollbacks.
        """

    # methods that are needed for integration with Apache Superset
    def get_schema_names(self, connection: SqlaConnection, **kw: Any):
        """
        Return the list of schemas.
        """
        return ["main"]

    def get_pk_constraint(
        self,
        connection: SqlaConnection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(
        self,
        connection: SqlaConnection,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ):
        return []

    get_check_constraints = get_foreign_keys
    get_indexes = get_foreign_keys
    get_unique_constraints = get_foreign_keys

    def get_table_comment(self, connection, table_name, schema=None, **kwargs):
        return {"text": ""}
