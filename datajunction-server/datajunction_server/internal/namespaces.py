"""
Helper methods for namespaces endpoints.
"""
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy import or_
from sqlalchemy.sql.operators import is_
from sqlmodel import Session, col, select

from datajunction_server.api.helpers import get_node_namespace, hard_delete_node
from datajunction_server.errors import (
    DJActionNotAllowedException,
    DJException,
    DJInvalidInputException,
)
from datajunction_server.models import History, User
from datajunction_server.models.history import ActivityType, EntityType
from datajunction_server.models.node import Node, NodeNamespace, NodeRevision, NodeType
from datajunction_server.typing import UTCDatetime
from datajunction_server.utils import SEPARATOR


def get_nodes_in_namespace(
    session: Session,
    namespace: str,
    node_type: NodeType = None,
    include_deactivated: bool = False,
) -> List[Dict]:
    """
    Gets a list of node names in the namespace
    """
    get_node_namespace(session, namespace)
    list_nodes_query = select(
        Node.name,
        NodeRevision.display_name,
        NodeRevision.description,
        Node.type,
        Node.current_version.label(  # type: ignore # pylint: disable=no-member
            "version",
        ),
        NodeRevision.status,
        NodeRevision.mode,
        NodeRevision.updated_at,
    ).where(
        or_(
            col(Node.namespace).like(f"{namespace}.%"),  # pylint: disable=no-member
            Node.namespace == namespace,
        ),
        Node.current_version == NodeRevision.version,
        Node.name == NodeRevision.name,
        Node.type == node_type if node_type else True,
    )
    if include_deactivated is False:
        list_nodes_query = list_nodes_query.where(is_(Node.deactivated_at, None))
    return session.exec(list_nodes_query).all()


def list_namespaces_in_hierarchy(  # pylint: disable=too-many-arguments
    session: Session,
    namespace: str,
) -> List[NodeNamespace]:
    """
    Get all namespaces in hierarchy under the specified namespace
    """
    statement = select(NodeNamespace).where(
        or_(
            col(NodeNamespace.namespace).like(  # pylint: disable=no-member
                f"{namespace}.%",
            ),
            NodeNamespace.namespace == namespace,
        ),
    )
    namespaces = session.exec(statement).all()
    if len(namespaces) == 0:
        raise DJException(
            message=(f"Namespace `{namespace}` does not exist."),
            http_status_code=404,
        )
    return namespaces


def mark_namespace_deactivated(
    session: Session,
    namespace: NodeNamespace,
    message: str = None,
    current_user: Optional[User] = None,
):
    """
    Deactivates the node namespace and updates history indicating so
    """
    now = datetime.utcnow()
    namespace.deactivated_at = UTCDatetime(
        year=now.year,
        month=now.month,
        day=now.day,
        hour=now.hour,
        minute=now.minute,
        second=now.second,
    )
    session.add(
        History(
            entity_type=EntityType.NAMESPACE,
            entity_name=namespace.namespace,
            node=None,
            activity_type=ActivityType.DELETE,
            details={"message": message or ""},
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()


def mark_namespace_restored(
    session: Session,
    namespace: NodeNamespace,
    message: str = None,
    current_user: Optional[User] = None,
):
    """
    Restores the node namespace and updates history indicating so
    """
    namespace.deactivated_at = None  # type: ignore
    session.add(
        History(
            entity_type=EntityType.NAMESPACE,
            entity_name=namespace.namespace,
            node=None,
            activity_type=ActivityType.RESTORE,
            details={"message": message or ""},
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()


def validate_namespace(namespace: str):
    """
    Validate that the namespace parts are valid (i.e., cannot start with numbers or be empty)
    """
    parts = namespace.split(SEPARATOR)
    for part in parts:
        if not part or (part and part[0].isdigit()):
            raise DJInvalidInputException(
                f"{namespace} is not a valid namespace. Namespace parts cannot start with "
                "numbers or be empty.",
            )


def get_parent_namespaces(namespace: str):
    """
    Return a list of all parent namespaces
    """
    parts = namespace.split(SEPARATOR)
    return [SEPARATOR.join(parts[0:i]) for i in range(len(parts)) if parts[0:i]]


def create_namespace(
    session: Session,
    namespace: str,
    include_parents: bool = True,
    current_user: Optional[User] = None,
) -> List[str]:
    """
    Creates a namespace entry in the database table.
    """
    parents = (
        get_parent_namespaces(namespace) + [namespace]
        if include_parents
        else [namespace]
    )
    for parent_namespace in parents:
        if not get_node_namespace(  # pragma: no cover
            session=session,
            namespace=parent_namespace,
            raise_if_not_exists=False,
        ):
            node_namespace = NodeNamespace(namespace=parent_namespace)
            session.add(node_namespace)
            session.add(
                History(
                    entity_type=EntityType.NAMESPACE,
                    entity_name=namespace,
                    node=None,
                    activity_type=ActivityType.CREATE,
                    user=current_user.username if current_user else None,
                ),
            )
    session.commit()
    return parents


def hard_delete_namespace(
    session: Session,
    namespace: str,
    cascade: bool = False,
    current_user: Optional[User] = None,
):
    """
    Hard delete a node namespace.
    """
    node_names = session.exec(
        select(Node.name).where(
            or_(
                col(Node.namespace).like(f"{namespace}.%"),  # pylint: disable=no-member
                Node.namespace == namespace,
            ),
        ),
    ).all()

    if not cascade and node_names:
        raise DJActionNotAllowedException(
            message=(
                f"Cannot hard delete namespace `{namespace}` as there are still the "
                f"following nodes under it: `{node_names}`. Set `cascade` to true to "
                "additionally hard delete the above nodes in this namespace. WARNING:"
                " this action cannot be undone."
            ),
        )

    impacts = {}
    for node_name in node_names:
        impacts[node_name] = hard_delete_node(
            node_name,
            session,
            current_user=current_user,
        )

    namespaces = list_namespaces_in_hierarchy(session, namespace)
    for _namespace in namespaces:
        impacts[_namespace.namespace] = {
            "namespace": _namespace.namespace,
            "status": "deleted",
        }
        session.delete(_namespace)
    return impacts
