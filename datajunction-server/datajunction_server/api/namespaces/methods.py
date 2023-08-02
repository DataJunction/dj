"""
Helper methods for namespaces endpoints.
"""
from datetime import datetime

from sqlalchemy import and_
from sqlalchemy.sql.operators import is_
from sqlmodel import Session, select

from datajunction_server.models import History
from datajunction_server.models.history import ActivityType, EntityType
from datajunction_server.models.node import Node, NodeNamespace, NodeType
from datajunction_server.typing import UTCDatetime


def get_nodes_in_namespace(
    session: Session,
    namespace: str,
    node_type: NodeType = None,
    deactivated: bool = False,
) -> list[str]:
    """
    Gets a list of node names in the namespace
    """
    where_clause = (
        and_(
            Node.namespace.like(  # type: ignore  # pylint: disable=no-member
                f"{namespace}%",
            ),
            Node.type == node_type,
        )
        if node_type
        else Node.namespace.like(  # type: ignore  # pylint: disable=no-member
            f"{namespace}%",
        )
    )

    list_nodes_query = select(Node.name).where(
        where_clause,
    )  # .where(is_(Node.deactivated_at, None))
    if deactivated is False:
        list_nodes_query = list_nodes_query.where(is_(Node.deactivated_at, None))
    return session.exec(list_nodes_query).all()


def mark_namespace_deactivated(session: Session, namespace: NodeNamespace):
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
            entity_name=namespace,
            node=None,
            activity_type=ActivityType.DELETE,
        ),
    )
    session.commit()


def mark_namespace_restored(session: Session, namespace: NodeNamespace):
    """
    Restores the node namespace and updates history indicating so
    """
    namespace.deactivated_at = None  # type: ignore
    session.add(
        History(
            entity_type=EntityType.NAMESPACE,
            entity_name=namespace,
            node=None,
            activity_type=ActivityType.RESTORE,
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
