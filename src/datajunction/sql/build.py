"""
Functions for building queries, from nodes or SQL.
"""

import ast
import operator
import re
from typing import Dict, List, Optional, Tuple

from sqlalchemy.engine import create_engine as sqla_create_engine
from sqlalchemy.schema import Column as SqlaColumn
from sqlalchemy.sql.elements import BinaryExpression
from sqlmodel import Session, select
from sqloxide import parse_sql

from datajunction.constants import DJ_DATABASE_ID
from datajunction.models.database import Database
from datajunction.models.node import Node
from datajunction.models.query import QueryCreate
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
from datajunction.typing import From, Identifier, ParseTree, Projection, Select
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

    # add groupbys to projection as well
    for groupby in groupbys:
        node_select.append_column(columns[groupby])

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
        new_from.append(subtree[0]["Query"]["body"]["Select"]["from"][0])

    return new_projection, new_from
