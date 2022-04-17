"""
Functions for building queries, from nodes or SQL.
"""

import ast
import operator
import re
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy.engine import create_engine as sqla_create_engine
from sqlalchemy.schema import Column as SqlaColumn
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.sql.expression import ClauseElement
from sqlmodel import Session, select
from sqloxide import parse_sql

from datajunction.constants import DEFAULT_DIMENSION_COLUMN
from datajunction.models.node import Node, NodeType
from datajunction.models.query import QueryCreate
from datajunction.sql.dag import (
    get_database_for_nodes,
    get_dimensions,
    get_referenced_columns_from_sql,
    get_referenced_columns_from_tree,
)
from datajunction.sql.parse import (
    find_nodes_by_key,
    find_nodes_by_key_with_parent,
    get_expression_from_projection,
)
from datajunction.sql.transpile import get_query, get_select_for_node
from datajunction.typing import (
    Expression,
    Identifier,
    Join,
    Projection,
    Relation,
    Select,
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


def get_dimensions_from_filters(filters: List[str]) -> Set[str]:
    """
    Extract dimensions from filters passed to the metric API.
    """
    dimensions: Set[str] = set()
    for filter_ in filters:
        match = FILTER_RE.match(filter_)
        if not match:
            raise Exception(f"Invalid filter: {filter_}")
        dimensions.add(match.group(1))
    return dimensions


def get_query_for_node(  # pylint: disable=too-many-locals
    session: Session,
    node: Node,
    groupbys: List[str],
    filters: List[str],
    database_id: Optional[int] = None,
) -> QueryCreate:
    """
    Return a DJ QueryCreate object from a given node.
    """
    # check that groupbys and filters are valid dimensions
    requested_dimensions = set(groupbys) | get_dimensions_from_filters(filters)
    valid_dimensions = set(get_dimensions(node))
    if not requested_dimensions <= valid_dimensions:
        invalid = sorted(requested_dimensions - valid_dimensions)
        plural = "s" if len(invalid) > 1 else ""
        raise Exception(f"Invalid dimension{plural}: {', '.join(invalid)}")

    # which columns are needed from the parents; this is used to determine the database
    # where the query will run
    referenced_columns = get_referenced_columns_from_sql(node.expression, node.parents)

    # extract all referenced dimensions so we can join the node with them
    dimensions: Dict[str, Node] = {}
    for dimension in requested_dimensions:
        name, column = dimension.rsplit(".", 1)
        if (
            name not in {parent.name for parent in node.parents}
            and name not in dimensions
        ):
            dimensions[name] = session.exec(select(Node).where(Node.name == name)).one()
            referenced_columns[name].add(column)

    # find database
    nodes = [node]
    nodes.extend(dimensions.values())
    database = get_database_for_nodes(session, nodes, referenced_columns, database_id)

    # base query
    node_select = get_select_for_node(node, database)
    source = node_select.froms[0]

    # join with dimensions
    for dimension in dimensions.values():
        subquery = get_select_for_node(
            dimension,
            database,
            referenced_columns[dimension.name],
        ).alias(dimension.name)
        condition = find_on_clause(node, source, dimension, subquery)
        node_select = node_select.select_from(source.join(subquery, condition))

    columns = {
        f"{column.table.name}.{column.name}": column
        for from_ in node_select.froms
        for column in from_.columns
    }

    # filter
    node_select = node_select.filter(
        *[get_filter(columns, filter_) for filter_ in filters]
    )

    # groupby
    node_select = node_select.group_by(*[columns[groupby] for groupby in groupbys])

    # add groupbys to projection as well
    for groupby in groupbys:
        node_select.append_column(columns[groupby])

    engine = sqla_create_engine(database.URI)
    sql = str(node_select.compile(engine, compile_kwargs={"literal_binds": True}))

    return QueryCreate(database_id=database.id, submitted_query=sql)


def find_on_clause(
    node: Node,
    node_select: Select,
    dimension: Node,
    subquery: Select,
) -> ClauseElement:
    """
    Return the on clause for a node/dimension selects.
    """
    for parent in node.parents:
        for column in parent.columns:
            if column.dimension == dimension:
                dimension_column = column.dimension_column or DEFAULT_DIMENSION_COLUMN
                return (
                    node_select.columns[column.name]  # type: ignore
                    == subquery.columns[dimension_column]  # type: ignore
                )

    raise Exception(f"Node {node.name} has no columns with dimension {dimension.name}")


# pylint: disable=too-many-branches, too-many-locals, too-many-statements
def get_query_for_sql(sql: str) -> QueryCreate:
    """
    Return a query given a SQL expression querying the repo.

    Eg:

        SELECT
            "core.users.gender", "core.num_comments"
        FROM metrics
        WHERE "core.comments.user_id" > 1
        GROUP BY
            "core.users.gender"

    This works by converting metrics (``core.num_comments``) into their selection
    definition (``COUNT(*)``), updating the sources to include the metrics parents
    (including joining with dimensions), and updating column references in the
    ``WHERE``, ``GROUP BY``, etc.
    """
    session = next(get_session())

    tree = parse_sql(sql, dialect="ansi")
    query_select = tree[0]["Query"]["body"]["Select"]

    # fetch all metric and dimension nodes
    nodes = {node.name: node for node in session.exec(select(Node))}

    # extract metrics and dimensions from the query
    identifiers = {
        identifier["value"]
        for identifier in find_nodes_by_key(query_select, "Identifier")
    }
    for compound_identifier in find_nodes_by_key(query_select, "CompoundIdentifier"):
        identifiers.add(".".join(part["value"] for part in compound_identifier))

    requested_metrics: Set[Node] = set()
    requested_dimensions: Set[Node] = set()
    for identifier in identifiers:
        if identifier in nodes and nodes[identifier].type == NodeType.METRIC:
            requested_metrics.add(nodes[identifier])
            continue

        if "." not in identifier:
            raise Exception(f"Invalid dimension: {identifier}")

        name, column = identifier.rsplit(".", 1)
        if name not in nodes:
            raise Exception(f"Invalid dimension: {identifier}")

        node = nodes[name]
        if node.type != NodeType.DIMENSION:
            continue

        column_names = {column.name for column in node.columns}
        if column not in column_names:
            raise Exception(f"Invalid dimension: {identifier}")

        requested_dimensions.add(node)

    # check that there is a metric with the superset of parents from all metrics
    main_metric = sorted(
        requested_metrics,
        key=lambda metric: (len(metric.parents), metric.name),
        reverse=True,
    )[0]
    for metric in requested_metrics:
        if not set(metric.parents) <= set(main_metric.parents):
            raise Exception(
                f"Metrics {metric.name} and {main_metric.name} have non-shared parents",
            )

    # replace the ``from`` part of the parse tree with the ``from`` from the metric that
    # has all the necessary parents
    metric_tree = parse_sql(main_metric.expression, dialect="ansi")
    query_select["from"] = metric_tree[0]["Query"]["body"]["Select"]["from"]

    # join to any dimensions
    for dimension in requested_dimensions:
        query_select["from"][0]["joins"].append(
            get_dimension_join(main_metric, dimension),
        )

    # update metric references in the projection
    projection = query_select["projection"]
    metric_names = {metric.name for metric in requested_metrics}
    for expression, parent in list(
        find_nodes_by_key_with_parent(projection, "UnnamedExpr"),
    ):
        replace_metric_identifier(expression, parent, nodes, metric_names)
    for expression_with_alias, parent in list(
        find_nodes_by_key_with_parent(projection, "ExprWithAlias"),
    ):
        alias = expression_with_alias["alias"]
        expression = expression_with_alias["expr"]
        replace_metric_identifier(expression, parent, nodes, metric_names, alias)

    # update metric references in ``HAVING``
    for identifier, parent in list(
        find_nodes_by_key_with_parent(query_select["having"], "Identifier"),
    ):
        name = identifier["value"]
        node = nodes[name]
        metric_tree = parse_sql(node.expression, dialect="ansi")
        parent.pop("Identifier")
        parent.update(
            get_expression_from_projection(
                metric_tree[0]["Query"]["body"]["Select"]["projection"][0],
            ),
        )

    # replace dimension references
    for part in ("projection", "selection", "group_by", "sort_by"):
        for identifier, parent in list(
            find_nodes_by_key_with_parent(query_select[part], "Identifier"),
        ):
            if identifier["value"] not in identifiers:
                continue

            name, column = identifier["value"].rsplit(".", 1)
            parent.pop("Identifier")
            parent["CompoundIdentifier"] = [
                {"quote_style": '"', "value": name},
                {"quote_style": '"', "value": column},
            ]

    parents = main_metric.parents + list(requested_dimensions)
    referenced_columns = get_referenced_columns_from_tree(tree, parents)

    database = get_database_for_nodes(session, parents, referenced_columns)
    query = get_query(None, parents, tree, database)
    engine = sqla_create_engine(database.URI)
    sql = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

    return QueryCreate(database_id=database.id, submitted_query=sql)


def replace_metric_identifier(
    expression: Expression,
    parent: Projection,
    nodes: Dict[str, Node],
    metric_names: Set[str],
    alias: Optional[Identifier] = None,
) -> None:
    """
    Replace any metric reference in ``expression`` with its SQL.
    """
    if "CompoundIdentifier" in expression:
        expression["Identifier"] = {
            "quote_style": None,
            "value": ".".join(
                part["value"] for part in expression.pop("CompoundIdentifier")
            ),
        }
    elif "Identifier" not in expression:
        return

    name = expression["Identifier"]["value"]
    if name not in metric_names:
        return

    # if this is an unnamed expression remove the key from the parent, since it will be
    # replaced with an expression with alias
    parent.pop("UnnamedExpr", None)

    node = nodes[name]
    metric_tree = parse_sql(node.expression, dialect="ansi")
    parent["ExprWithAlias"] = {
        "alias": alias or {"quote_style": '"', "value": node.name},
        "expr": get_expression_from_projection(
            metric_tree[0]["Query"]["body"]["Select"]["projection"][0],
        ),
    }


def get_join_columns(node: Node, dimension: Node) -> Tuple[str, str, str]:
    """
    Return the columns to perform a join between a node and a dimension.
    """
    for parent in node.parents:
        for column in parent.columns:
            if column.dimension == dimension:
                return (
                    parent.name,
                    column.name,
                    column.dimension_column or DEFAULT_DIMENSION_COLUMN,
                )

    raise Exception(f"Node {node.name} has no columns with dimension {dimension.name}")


def get_dimension_join(node: Node, dimension: Node) -> Join:
    """
    Return the join between a node and a dimension.
    """
    parent_name, node_column, dimension_column = get_join_columns(node, dimension)
    relation: Relation = {
        "Table": {
            "alias": None,
            "args": [],
            "name": [{"quote_style": None, "value": dimension.name}],
            "with_hints": [],
        },
    }

    return {
        "join_operator": {
            "Inner": {
                "On": {
                    "BinaryOp": {
                        "left": {
                            "CompoundIdentifier": [
                                {"quote_style": None, "value": parent_name},
                                {"quote_style": None, "value": node_column},
                            ],
                        },
                        "op": "Eq",
                        "right": {
                            "CompoundIdentifier": [
                                {"quote_style": None, "value": dimension.name},
                                {"quote_style": None, "value": dimension_column},
                            ],
                        },
                    },
                },
                "Using": [],
            },
        },
        "relation": relation,
    }
