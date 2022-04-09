"""
Database-specific fixes.
"""

from pydruid.db.sqlalchemy import DruidDialect, get_default, get_is_nullable, type_map
from sqlalchemy import text


def patch_druid_get_columns() -> None:
    """
    Patch Druid's ``get_columns`` to work with SQLAlchemy 1.4.

    This needs to be done until https://github.com/druid-io/pydruid/pull/275/files is
    released in a new version.
    """

    # pylint: disable=unused-argument
    def get_columns_fixed(self, connection, table_name, schema=None, **kwargs):
        query = f"""
            SELECT COLUMN_NAME,
                   DATA_TYPE,
                   IS_NULLABLE,
                   COLUMN_DEFAULT
              FROM INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_NAME = '{table_name}'
        """
        if schema:
            query = f"{query} AND TABLE_SCHEMA = '{schema}'"

        result = connection.execute(text(query))

        return [
            {
                "name": row.COLUMN_NAME,
                "type": type_map[row.DATA_TYPE.lower()],
                "nullable": get_is_nullable(row.IS_NULLABLE),
                "default": get_default(row.COLUMN_DEFAULT),
            }
            for row in result
        ]

    DruidDialect.get_columns = get_columns_fixed
