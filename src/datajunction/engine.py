"""
Query related functions.
"""

import ast
import operator
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import sqlparse
from sqlalchemy import text
from sqlalchemy.engine import create_engine as sqla_create_engine
from sqlalchemy.schema import Column as SqlaColumn
from sqlalchemy.sql.elements import BinaryExpression
from sqlmodel import Session, create_engine, select
from sqloxide import parse_sql

from datajunction.config import Settings
from datajunction.constants import DJ_DATABASE_ID
from datajunction.models.database import Database
from datajunction.models.node import Node
from datajunction.models.query import (
    ColumnMetadata,
    Query,
    QueryCreate,
    QueryResults,
    QueryState,
    QueryWithResults,
    StatementResults,
)
from datajunction.sql.dag import (
    get_computable_databases,
    get_referenced_columns_from_tree,
)
from datajunction.sql.parse import (
    find_nodes_by_key,
    find_nodes_by_key_with_parent,
    get_expression_from_projection,
    is_metric,
)
from datajunction.sql.transpile import get_query, get_select_for_node
from datajunction.typing import (
    ColumnType,
    Description,
    From,
    Identifier,
    ParseTree,
    Projection,
    Select,
    SQLADialect,
    Stream,
    TypeEnum,
)
from datajunction.utils import get_session

FILTER_RE = re.compile(r"([\w\./_]+)(<=|<|>=|>|!=|=)(.+)")
COMPARISONS = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "=": operator.eq,
    "!=": operator.ne,
}


def get_filter(columns: Dict[str, SqlaColumn], filter_: str) -> BinaryExpression:
    """
    Build a SQLAlchemy filter.
    """
    match = FILTER_RE.match(filter_)
    if not match:
        raise Exception(f"Invalid filter: {filter_}")

    name, op, value = match.groups()  # pylint: disable=invalid-name

    if name not in columns:
        raise Exception(f"Invalid column name: {name}")
    column = columns[name]

    if op not in COMPARISONS:
        valid = ", ".join(COMPARISONS)
        raise Exception(f"Invalid operation: {op} (valid: {valid})")
    comparison = COMPARISONS[op]

    try:
        value = ast.literal_eval(value)
    except Exception as ex:
        raise Exception(f"Invalid value: {value}") from ex

    return comparison(column, value)


def get_query_for_node(
    node: Node,
    groupbys: List[str],
    filters: List[str],
    database_id: Optional[int] = None,
) -> QueryCreate:
    """
    Return a DJ QueryCreate object from a given node.
    """
    databases = get_computable_databases(node)
    if not databases:
        raise Exception(f"Unable to compute {node.name} (no common database)")
    if database_id:
        for database in databases:
            if database.id == database_id:
                break
        else:
            raise Exception(f"Unable to compute {node.name} on database {database_id}")
    else:
        database = sorted(databases, key=operator.attrgetter("cost"))[0]

    engine = sqla_create_engine(database.URI)
    node_select = get_select_for_node(node, database)

    columns = {
        f"{from_.name}.{column.name}": column
        for from_ in node_select.froms
        for column in from_.columns
    }
    node_select = node_select.filter(
        *[get_filter(columns, filter_) for filter_ in filters]
    ).group_by(*[columns[groupby] for groupby in groupbys])

    sql = str(node_select.compile(engine, compile_kwargs={"literal_binds": True}))

    return QueryCreate(database_id=database.id, submitted_query=sql)


def get_query_for_sql(sql: str) -> QueryCreate:
    """
    Return a query given a SQL expression querying the repo.

    Eg:

        SELECT "core.num_comments" FROM metrics
        WHERE "core.comments.user_id" > 1
        GROUP BY "core.comments.user_id"

    """
    session = next(get_session())

    tree = parse_sql(sql, dialect="ansi")
    query_select = tree[0]["Query"]["body"]["Select"]

    # all metrics should share the same parent(s)
    parents: List[Node] = []

    # replace metrics with their definitions
    projection = next(find_nodes_by_key(tree, "projection"))
    new_projection, new_from = get_new_projection_and_from(session, projection, parents)
    query_select.update({"from": new_from, "projection": new_projection})

    # replace dimensions with column references
    replace_dimension_references(query_select, parents)

    database = get_database_for_sql(session, tree, parents)
    query = get_query(None, parents, tree, database)
    engine = sqla_create_engine(database.URI)
    sql = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

    return QueryCreate(database_id=database.id, submitted_query=sql)


def replace_dimension_references(query_select: Select, parents: List[Node]) -> None:
    """
    Update a query inplace, replacing dimensions with proper column names.
    """
    for part in ("projection", "selection", "group_by", "sort_by"):
        for identifier, parent in list(
            find_nodes_by_key_with_parent(query_select[part], "Identifier"),  # type: ignore
        ):
            if "." not in identifier["value"]:
                # metric already processed
                continue

            name, column = identifier["value"].rsplit(".", 1)
            if name not in {parent.name for parent in parents}:
                raise Exception(f"Invalid identifier: {name}")

            parent.pop("Identifier")
            parent["CompoundIdentifier"] = [
                {"quote_style": '"', "value": name},
                {"quote_style": '"', "value": column},
            ]


def get_database_for_sql(
    session: Session,
    tree: ParseTree,
    parents: List[Node],
) -> Database:
    """
    Given a list of parents, return the best database to compute metric.

    When no parents are passed, the database with the lowest cost is returned.
    """
    if parents:
        parent_columns = get_referenced_columns_from_tree(tree, parents)
        databases = set.intersection(
            *[
                get_computable_databases(parent, parent_columns[parent.name])
                for parent in parents
            ]
        )
    else:
        databases = session.exec(
            select(Database).where(Database.id != DJ_DATABASE_ID),
        ).all()

    if not databases:
        raise Exception("Unable to run SQL (no common database)")
    return sorted(databases, key=operator.attrgetter("cost"))[0]


def get_new_projection_and_from(
    session: Session,
    projection: List[Projection],
    parents: List[Node],
) -> Tuple[List[Projection], List[From]]:
    """
    Replace node references in the ``SELECT`` clause and update ``FROM`` clause.

    Node names in the ``SELECT`` clause are replaced by the corresponding node expression
    (only the ``SELECT`` part), while the ``FROM`` clause is updated to point to the
    node parent(s).

    This assumes that all nodes referenced in the ``SELECT`` share the same parents.
    """
    new_from: List[From] = []
    new_projection: List[Projection] = []
    for projection_expression in projection:
        alias: Optional[Identifier] = None
        if "UnnamedExpr" in projection_expression:
            expression = projection_expression["UnnamedExpr"]
        elif "ExprWithAlias" in projection_expression:
            expression = projection_expression["ExprWithAlias"]["expr"]
            alias = projection_expression["ExprWithAlias"]["alias"]
        else:
            raise NotImplementedError(f"Unable to handle expression: {expression}")

        if "Identifier" in expression:
            name = expression["Identifier"]["value"]
        elif "CompoundIdentifier" in expression:
            name = ".".join(part["value"] for part in expression["CompoundIdentifier"])
        else:
            new_projection.append(projection_expression)
            continue

        if alias is None:
            alias = {
                "quote_style": '"',
                "value": name,
            }

        node = session.exec(select(Node).where(Node.name == name)).one_or_none()
        if node is None:
            # skip, since this is probably a dimension
            new_projection.append(projection_expression)
            continue
        if not node.expression or not is_metric(node.expression):
            raise Exception(f"Not a valid metric: {name}")
        if not parents:
            parents.extend(node.parents)
        elif set(parents) != set(node.parents):
            raise Exception("All metrics should have the same parents")

        subtree = parse_sql(node.expression, dialect="ansi")
        new_projection.append(
            {
                "ExprWithAlias": {
                    "alias": alias,
                    "expr": get_expression_from_projection(
                        subtree[0]["Query"]["body"]["Select"]["projection"][0],
                    ),
                },
            },
        )
        new_from.append(subtree[0]["Query"]["body"]["Select"]["from"])

    return new_projection, new_from


def get_columns_from_description(
    description: Description,
    dialect: SQLADialect,
) -> List[ColumnMetadata]:
    """
    Extract column metadata from the cursor description.

    For now this uses the information from the cursor description, which only allow us to
    distinguish between 4 types (see ``TypeEnum``). In the future we should use a type
    inferrer to determine the types based on the query.
    """
    type_map = {
        TypeEnum.STRING: ColumnType.STR,
        TypeEnum.BINARY: ColumnType.BYTES,
        TypeEnum.NUMBER: ColumnType.FLOAT,
        TypeEnum.DATETIME: ColumnType.DATETIME,
    }

    columns = []
    for column in description or []:
        name, native_type = column[:2]
        for dbapi_type in TypeEnum:
            if native_type == getattr(dialect.dbapi, dbapi_type.value, None):
                type_ = type_map[dbapi_type]
                break
        else:
            # fallback to string
            type_ = ColumnType.STR

        columns.append(ColumnMetadata(name=name, type=type_))

    return columns


def run_query(query: Query) -> List[Tuple[str, List[ColumnMetadata], Stream]]:
    """
    Run a query and return its results.

    For each statement we return a tuple with the statement SQL, a description of the
    columns (name and type) and a stream of rows (tuples).
    """
    engine = create_engine(query.database.URI)
    connection = engine.connect()

    output: List[Tuple[str, List[ColumnMetadata], Stream]] = []
    statements = sqlparse.parse(query.executed_query)
    for statement in statements:
        # Druid doesn't like statements that end in a semicolon...
        sql = str(statement).strip().rstrip(";")

        results = connection.execute(text(sql))
        stream = (tuple(row) for row in results)
        columns = get_columns_from_description(
            results.cursor.description,
            engine.dialect,
        )
        output.append((sql, columns, stream))

    return output


def process_query(
    session: Session,
    settings: Settings,
    query: Query,
) -> QueryWithResults:
    """
    Process a query.
    """
    query.scheduled = datetime.now(timezone.utc)
    query.state = QueryState.SCHEDULED
    query.executed_query = query.submitted_query

    errors = []
    query.started = datetime.now(timezone.utc)
    try:
        root = []
        for sql, columns, stream in run_query(query):
            rows = list(stream)
            root.append(
                StatementResults(
                    sql=sql,
                    columns=columns,
                    rows=rows,
                    row_count=len(rows),
                ),
            )
        results = QueryResults(__root__=root)

        query.state = QueryState.FINISHED
        query.progress = 1.0
    except Exception as ex:  # pylint: disable=broad-except
        results = QueryResults(__root__=[])
        query.state = QueryState.FAILED
        errors = [str(ex)]

    query.finished = datetime.now(timezone.utc)

    session.add(query)
    session.commit()
    session.refresh(query)

    settings.results_backend.add(str(query.id), results.json())

    return QueryWithResults(results=results, errors=errors, **query.dict())
