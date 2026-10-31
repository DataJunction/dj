"""Resolve physical catalog names for direct query clients."""

from urllib.parse import parse_qs, urlparse

import sqlglot
from sqlglot import exp

from datajunction_server.models.dialect import Dialect


def catalog_from_engine_uri(uri: str | None, dialect: Dialect) -> str | None:
    """Return the project or database encoded in an engine URI, if present."""
    if not uri:
        return None
    parsed = urlparse(uri)
    params = parse_qs(parsed.query)
    path_part = parsed.path.strip("/").split("/", 1)[0]
    if dialect == Dialect.BIGQUERY:
        return (
            parsed.hostname or path_part or next(iter(params.get("project", [])), None)
        )
    if dialect == Dialect.SNOWFLAKE:
        return path_part or next(iter(params.get("database", [])), None)
    return None


def replace_catalog_alias(
    sql: str,
    alias: str,
    physical_catalog: str,
    dialect: Dialect,
) -> str:
    """Change catalog qualifiers on SQL tables without touching CTEs or literals."""
    if alias == physical_catalog:
        return sql
    expression = sqlglot.parse_one(sql, read=dialect.value)
    changed = False
    for table in expression.find_all(exp.Table):
        if table.catalog == alias:
            original = table.args["catalog"]
            table.set(
                "catalog",
                exp.to_identifier(
                    physical_catalog,
                    quoted=bool(original.args.get("quoted")),
                ),
            )
            changed = True
    return expression.sql(dialect=dialect.value) if changed else sql
