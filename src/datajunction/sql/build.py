"""
Build query related functions.
"""
import operator
from typing import List, Optional, Tuple

from sqlalchemy.engine import create_engine as sqla_create_engine
from sqlmodel import Session, create_engine, select
from sqloxide import parse_sql

from datajunction.constants import DJ_DATABASE_ID
from datajunction.models.database import Database
from datajunction.models.node import Node
from datajunction.models.query import (

    QueryCreate,

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
    From,
    Identifier,
    ParseTree,
    Projection,
    Select,
)
from datajunction.utils import get_session


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

    # replace dimensions with column referencs
    replace_dimension_references(query_select, parents)

    database = get_database_for_sql(session, tree, parents)
    query = get_query(None, parents, tree, database)
    engine = sqla_create_engine(database.URI)
    sql = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

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

    # replace dimensions with column referencs
    replace_dimension_references(query_select, parents)

    database = get_database_for_sql(session, tree, parents)
    query = get_query(None, parents, tree, database)
    engine = sqla_create_engine(database.URI)
    sql = str(query.compile(engine, compile_kwargs={"literal_binds": True}))

    return QueryCreate(database_id=database.id, submitted_query=sql)


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
