"""
Helper methods for namespaces endpoints.
"""
from datetime import datetime
from typing import Dict, List

from sqlalchemy.sql.operators import is_
from sqlmodel import Session, col, select

from datajunction_server.models import History
from datajunction_server.models.history import ActivityType, EntityType
from datajunction_server.models.node import Node, NodeNamespace, NodeRevision, NodeType
from datajunction_server.typing import UTCDatetime


def get_nodes_in_namespace(
    session: Session,
    namespace: str,
    node_type: NodeType = None,
    include_deactivated: bool = False,
) -> List[Dict]:
    """
    Gets a list of node names in the namespace
    """
    list_nodes_query = select(
        Node.name,
        Node.display_name,
        Node.type,
        Node.current_version.label(  # type: ignore # pylint: disable=no-member
            "version",
        ),
        NodeRevision.status,
        NodeRevision.mode,
        NodeRevision.updated_at,
    ).where(
        col(Node.namespace).contains(namespace),  # pylint: disable=no-member
        Node.current_version == NodeRevision.version,
        Node.name == NodeRevision.name,
        Node.type == node_type if node_type else True,
    )
    if include_deactivated is False:
        list_nodes_query = list_nodes_query.where(is_(Node.deactivated_at, None))
    return session.exec(list_nodes_query).all()


def mark_namespace_deactivated(
    session: Session,
    namespace: NodeNamespace,
    message: str = None,
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
        ),
    )
    session.commit()


def mark_namespace_restored(
    session: Session,
    namespace: NodeNamespace,
    message: str = None,
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
        ),
    )
    session.commit()


def create_namespace(session: Session, namespace: str):
    """
    Creates a namespace entry in the database table.
    """
    node_namespace = NodeNamespace(namespace=namespace)
    session.add(node_namespace)
    session.add(
        History(
            entity_type=EntityType.NAMESPACE,
            entity_name=namespace,
            node=None,
            activity_type=ActivityType.CREATE,
        ),
    )
    session.commit()
