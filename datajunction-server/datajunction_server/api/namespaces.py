"""
Node namespace related APIs.
"""
import logging
from http import HTTPStatus
from typing import List, Optional

from fastapi import Depends, Query
from fastapi.responses import JSONResponse
from sqlalchemy.sql.operators import is_
from sqlmodel import Session, select

from datajunction_server.api.helpers import (
    activate_node,
    deactivate_node,
    get_node_namespace,
)
from datajunction_server.errors import DJAlreadyExistsException
from datajunction_server.internal.authentication.http import SecureAPIRouter
from datajunction_server.internal.namespaces import (
    create_namespace,
    get_nodes_in_namespace,
    mark_namespace_deactivated,
    mark_namespace_restored,
)
from datajunction_server.models.node import NodeNameList, NodeNamespace, NodeType
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["namespaces"])


@router.post("/namespaces/{namespace}/", status_code=HTTPStatus.CREATED)
def create_node_namespace(
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
    create_namespace(session, namespace)
    return JSONResponse(
        status_code=HTTPStatus.CREATED,
        content={
            "message": (f"Node namespace `{namespace}` has been successfully created"),
        },
    )


@router.get(
    "/namespaces/",
    response_model=List[str],
    status_code=200,
)
def list_namespaces(
    session: Session = Depends(get_session),
) -> List[str]:
    """
    List namespaces
    """
    return session.exec(
        select(NodeNamespace.namespace).where(is_(NodeNamespace.deactivated_at, None)),
    ).all()


@router.get(
    "/namespaces/{namespace}/",
    response_model=NodeNameList,
    status_code=200,
)
def list_nodes_in_namespace(
    namespace: str,
    type_: Optional[NodeType] = Query(
        default=None,
        description="Filter the list of nodes to this type",
    ),
    session: Session = Depends(get_session),
) -> NodeNameList:
    """
    List node names in namespace, filterable to a given type if desired.
    """
    return get_nodes_in_namespace(session, namespace, type_)  # type: ignore


@router.delete("/namespaces/{namespace}/", status_code=HTTPStatus.OK)
def deactivate_a_namespace(
    namespace: str,
    cascade: bool = Query(
        default=False,
        description="Cascade the deletion down to the nodes in the namespace",
    ),
    session: Session = Depends(get_session),
) -> JSONResponse:
    """
    Deactivates a node namespace
    """
    node_namespace = get_node_namespace(
        session=session,
        namespace=namespace,
        raise_if_not_exists=True,
    )

    if node_namespace.deactivated_at:
        raise DJAlreadyExistsException(
            message=f"Namespace `{namespace}` is already deactivated.",
        )

    # If there are no active nodes in the namespace, we can safely deactivate this namespace
    node_names = get_nodes_in_namespace(session, namespace)
    if len(node_names) == 0:
        message = f"Namespace `{namespace}` has been deactivated."
        mark_namespace_deactivated(session, node_namespace, message)
        return JSONResponse(
            status_code=HTTPStatus.OK,
            content={"message": message},
        )

    # If cascade=true is set, we'll deactivate all nodes in this namespace and then
    # subsequently deactivate this namespace
    if cascade:
        for node_name in node_names:
            deactivate_node(
                session,
                node_name,
                f"Cascaded from deactivating namespace `{namespace}`",
            )
        message = (
            f"Namespace `{namespace}` has been deactivated. The following nodes"
            f" have also been deactivated: {','.join(node_names)}"
        )
        mark_namespace_deactivated(session, node_namespace, message)

        return JSONResponse(
            status_code=HTTPStatus.OK,
            content={
                "message": message,
            },
        )

    return JSONResponse(
        status_code=405,
        content={
            "message": f"Cannot deactivate node namespace `{namespace}` as there are "
            "still active nodes under that namespace.",
        },
    )


@router.post("/namespaces/{namespace}/restore/", status_code=HTTPStatus.CREATED)
def restore_a_namespace(
    namespace: str,
    cascade: bool = Query(
        default=False,
        description="Cascade the restore down to the nodes in the namespace",
    ),
    session: Session = Depends(get_session),
) -> JSONResponse:
    """
    Restores a node namespace
    """
    node_namespace = get_node_namespace(
        session=session,
        namespace=namespace,
        raise_if_not_exists=True,
    )
    if not node_namespace.deactivated_at:
        raise DJAlreadyExistsException(
            message=f"Node namespace `{namespace}` already exists and is active.",
        )

    node_names = get_nodes_in_namespace(session, namespace, include_deactivated=True)

    # If cascade=true is set, we'll restore all nodes in this namespace and then
    # subsequently restore this namespace
    if cascade:
        for node_name in node_names:
            activate_node(
                name=node_name,
                session=session,
                message=f"Cascaded from restoring namespace `{namespace}`",
            )

        message = (
            f"Namespace `{namespace}` has been restored. The following nodes"
            f" have also been restored: {','.join(node_names)}"
        )
        mark_namespace_restored(session, node_namespace, message)

        return JSONResponse(
            status_code=HTTPStatus.CREATED,
            content={
                "message": message,
            },
        )

    # Otherwise just restore this namespace
    message = f"Namespace `{namespace}` has been restored."
    mark_namespace_restored(session, node_namespace, message)
    return JSONResponse(
        status_code=HTTPStatus.CREATED,
        content={"message": message},
    )
