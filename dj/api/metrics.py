"""
Metric related APIs.
"""

from http import HTTPStatus
from typing import List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Response
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, SQLModel, select

from dj.api.helpers import get_query
from dj.config import Settings
from dj.models.node import Node, NodeType
from dj.models.query import QueryCreate, QueryWithResults
from dj.sql.dag import get_dimensions
from dj.utils import UTCDatetime, get_session, get_settings

router = APIRouter()


class Metric(SQLModel):
    """
    Class for a metric.
    """

    id: int
    name: str
    display_name: str
    current_version: str
    description: str = ""

    created_at: UTCDatetime
    updated_at: UTCDatetime

    query: str

    dimensions: List[str]

    @classmethod
    def parse_node(cls, node: Node) -> "Metric":
        """
        Parses a node into a metric.
        """

        return cls(
            **node.dict(),
            description=node.current.description,
            updated_at=node.current.updated_at,
            query=node.current.query,
            dimensions=get_dimensions(node),
        )


class TranslatedSQL(SQLModel):
    """
    Class for SQL generated from a given metric.
    """

    database_id: int
    sql: str


def get_metric(session: Session, name: str) -> Node:
    """
    Return a metric node given a node name.
    """
    statement = select(Node).where(Node.name == name)
    try:
        node = session.exec(statement).one()
        if node.type != NodeType.METRIC:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail=f"Not a metric node: `{name}`",
            )
    except NoResultFound as exc:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Metric node not found: `{name}`",
        ) from exc
    return node


@router.get("/metrics/", response_model=List[Metric])
def read_metrics(*, session: Session = Depends(get_session)) -> List[Metric]:
    """
    List all available metrics.
    """
    return [
        Metric.parse_node(node)
        for node in (
            session.exec(
                select(Node).where(Node.type == NodeType.METRIC),
            )
        )
    ]


@router.get("/metrics/{name}/", response_model=Metric)
def read_metric(name: str, *, session: Session = Depends(get_session)) -> Metric:
    """
    Return a metric by name.
    """
    node = get_metric(session, name)
    return Metric.parse_node(node)


@router.get("/metrics/{name}/sql/", response_model=TranslatedSQL)
async def read_metrics_sql(
    name: str,
    dimensions: List[str] = Query([]),  # pylint: disable=invalid-name
    filters: List[str] = Query([]),  # pylint: disable=invalid-name
    database_name: Optional[str] = None,
    *,
    session: Session = Depends(get_session),
) -> TranslatedSQL:
    """
    Return SQL for a metric.

    A database can be optionally specified. If no database is specified the optimal one
    will be used.
    """
    query_ast, optimal_database = await get_query(
        session=session,
        metric=name,
        dimensions=dimensions,
        filters=filters,
        database_name=database_name,
    )
    return TranslatedSQL(
        database_id=optimal_database.id,
        sql=str(query_ast),
    )
