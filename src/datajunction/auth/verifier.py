"""
Authorization verification.
"""

from typing import Dict, List, Optional

from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqloxide import parse_sql

from datajunction.sql.parse import find_nodes_by_key, find_nodes_by_key_with_parent
from datajunction.typing import ParseTree, Statement, Table

NULL = "null"


# 1. convert aliased expressions to unaliased (in both)
# 2. check that all expressions with identifiers in query are in permissions; OR that ALL their sub-expressions are
# 3. check that all groups in the permission are in the expression
# 4. check that WHERE/HAVING in the permission are in the expression (as is or stricter)
# 5. optionally apply WHERE/HAVING if not present


def verify_query(
    query: str,
    engine: Engine,
    database: str,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    permissions=List[str],
) -> bool:
    """
    Verify if a query meets the criteria from a list of permissions.

    The query is composed of multiple statements, all of which need to match the list of
    permissions.
    """
    tree = parse_sql(query, dialect="ansi")
    prepare_tree(tree, engine, database, catalog, schema)
    return all(verify_statement(statement, permissions) for statement in tree)


def verify_statement(query: Statement, permissions: List[str]) -> bool:
    """
    Verify if a given query meets the criteria from a list of permissions.

    The query must match at least one statement in each of the permissions, eg:

        >>> permissions = ['SELECT * FROM foo; SELECT * FROM bar', 'SELECT a FROM foo']
        >>> statement = parse_sql('SELECT AVG(a) FROM foo', dialect='ansi')[0]
        >>> verify_statement(statement, permissions)
        True

    """
    return all(
        any(
            matches_permission(query, permission_statement)
            for permission_statement in parse_sql(permission, dialect="ansi")
        )
        for permission in permissions
    )


def matches_permission(query: Statement, permission: Statement) -> bool:
    """
    Check if a query can be satisfied by a given permission.
    """
    # check if tables in the query are allowed by the permission
    allowed_tables = [table["name"] for table in find_nodes_by_key(permission, "Table")]
    for table in find_nodes_by_key(query, "Table"):
        match = False
        for allowed_table in allowed_tables:
            for left, right in zip(allowed_table, table["name"]):
                if left["value"] == NULL:
                    left["value"] = None
                if left != right:
                    break
            else:
                match = True

        if not match:
            return False

    # check if columns in the query are allowed by the permission
    for projection in find_nodes_by_key(query, 'projection'):

    return True


def get_column_names(engine: Engine, fq_table: Table) -> List[str]:
    """
    Return the column names of a given table.
    """
    *_, schema, table = [part["value"] for part in fq_table["name"]]
    inspector = inspect(engine)
    column_metadata = inspector.get_columns(table, schema=schema)
    return [column["name"] for column in column_metadata]


def prepare_tree(  # pylint: disable=too-many-locals
    tree: ParseTree,
    engine: Engine,
    database: str,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> None:
    """
    Make sure all tables and columns in a parse tree are fully qualified and unaliased.

    Fully qualified identifiers include database, catalog, schema, and table:

        > SELECT trino_dev.hive.public.users.first_name
          FROM trino_dev.hive.public.users

    """
    prefix = [
        {"quote_style": None, "value": value} for value in [database, catalog, schema]
    ]

    # qualify tables and store their columns for non-qualified columns
    columns: Dict[str, List[str]] = {}
    tables: Dict[str, Table] = {}
    for table in find_nodes_by_key(tree, "Table"):
        name = table["name"]
        table["name"] = prefix[: 4 - len(name)] + name

        key = ".".join(part["value"] or NULL for part in table["name"])
        columns[key] = get_column_names(engine, table)
        tables[key] = table

    # qualify columns; we do this for each ``SELECT`` since we need to take aliases in
    # consideration
    for select in find_nodes_by_key(tree, "Select"):
        table_aliases: Dict[str, List[str]] = {}
        for table in find_nodes_by_key(select, "Table"):
            if table["alias"] is not None:
                table_aliases[table["alias"]["name"]["value"]] = table["name"]
                table["alias"] = None

        # qualify columns that are compound identifiers
        for cid, parent in find_nodes_by_key_with_parent(select, "CompoundIdentifier"):
            *_, table, column = cid

            if table["value"] in table_aliases:
                cid = table_aliases[table["value"]] + [column]
            else:
                cid = prefix[: 5 - len(cid)] + cid
            parent["CompoundIdentifier"] = cid

        # qualify non-compound identifiers by figuring out which table they come from
        for id_, parent in list(find_nodes_by_key_with_parent(select, "Identifier")):
            candidates = [
                key for key, value in columns.items() if id_["value"] in value
            ]
            if not candidates:
                raise Exception(f"Column {id_} not found in any table")
            if len(candidates) > 1:
                raise Exception(f"Column {id_} present is ambiguous")
            table = tables[candidates[0]]
            parent["CompoundIdentifier"] = table["name"] + [id_]
            parent.pop("Identifier")

    # remove aliases
    for expression_with_alias, parent in list(
        find_nodes_by_key_with_parent(
            tree,
            "ExprWithAlias",
        )
    ):
        parent["UnnamedExpr"] = expression_with_alias["expr"]
        parent.pop("ExprWithAlias")
