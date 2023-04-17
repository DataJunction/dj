"""
Node namespace related APIs.
"""
import logging
from typing import List

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import joinedload
from sqlmodel import Session, select

from dj.api.helpers import get_node_namespace
from dj.models.node import Node, NodeNamespace, NodeOutput
from dj.utils import get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/namespaces/{namespace}/", status_code=201)
def create_a_node_namespace(
    namespace: str,
    session: Session = Depends(get_session),
) -> JSONResponse:
    """
    Create a node namespace
    """
    if get_node_namespace(
        session=session,
        namespace=namespace,
        raise_if_not_exists=False,
    ):  # pragma: no cover
        return JSONResponse(
            status_code=409,
            content={
                "message": (f"Node namespace `{namespace}` already exists"),
            },
        )
    node_namespace = NodeNamespace(namespace=namespace)
    session.add(node_namespace)
    session.commit()
    return JSONResponse(
        status_code=201,
        content={
            "message": (f"Node namespace `{namespace}` has been successfully created"),
        },
    )


@router.get(
    "/namespaces/",
    response_model=List[NodeNamespace],
    status_code=200,
)
def list_node_namespaces(
    session: Session = Depends(get_session),
) -> List[NodeNamespace]:
    """
    List node namespaces
    """
    namespaces = session.exec(select(NodeNamespace)).all()
    return namespaces


@router.get(
    "/namespaces/{namespace}/",
    response_model=List[NodeOutput],
    status_code=200,
)
def list_nodes_in_namespace(
    namespace: str,
    session: Session = Depends(get_session),
) -> List[NodeOutput]:
    """
    List nodes in namespace
    """
    nodes = (
        session.exec(
            select(Node)
            .options(joinedload(Node.current))
            .where(
                Node.namespace.like(  # type: ignore  # pylint: disable=no-member
                    f"{namespace}%",
                ),
            ),
        )
        .unique()
        .all()
    )
    return nodes  # type: ignore
