"""Namespace database schema."""
from typing import List, Optional

from sqlalchemy import DateTime, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, joinedload, load_only, mapped_column, selectinload
from sqlalchemy.sql.operators import is_, or_

from datajunction_server.database.base import Base
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import DJDoesNotExistException
from datajunction_server.models.node import NodeMinimumDetail
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import _node_output_options
from datajunction_server.typing import UTCDatetime


class NodeNamespace(Base):  # pylint: disable=too-few-public-methods
    """
    A node namespace
    """

    __tablename__ = "nodenamespace"

    namespace: Mapped[str] = mapped_column(
        nullable=False,
        unique=True,
        primary_key=True,
    )
    deactivated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )

    @classmethod
    async def get_all_with_node_count(cls, session: AsyncSession):
        """
        Get all namespaces with the number of nodes in that namespaces.
        """
        statement = (
            select(
                NodeNamespace.namespace,
                func.count(Node.id).label("num_nodes"),  # pylint: disable=not-callable
            )
            .join(
                Node,
                onclause=NodeNamespace.namespace == Node.namespace,
                isouter=True,
            )
            .where(
                is_(NodeNamespace.deactivated_at, None),
            )
            .group_by(NodeNamespace.namespace)
        )
        result = await session.execute(statement)
        return result.all()

    @classmethod
    async def get(
        cls,
        session: AsyncSession,
        namespace: str,
        raise_if_not_exists: bool = True,
    ) -> Optional["NodeNamespace"]:
        """
        List node names in namespace.
        """
        statement = select(cls).where(cls.namespace == namespace)
        results = await session.execute(statement)
        node_namespace = results.scalar_one_or_none()
        if raise_if_not_exists:  # pragma: no cover
            if not node_namespace:
                raise DJDoesNotExistException(
                    message=(f"node namespace `{namespace}` does not exist."),
                    http_status_code=404,
                )
        return node_namespace

    @classmethod
    async def list_nodes(
        cls,
        session: AsyncSession,
        namespace: str,
        node_type: Optional[NodeType] = None,
        include_deactivated: bool = False,
        with_edited_by: bool = False,
    ) -> List["NodeMinimumDetail"]:
        """
        List node names in namespace.
        """
        await cls.get(session, namespace)

        list_nodes_query = (
            select(Node)
            .where(
                or_(
                    Node.namespace.like(f"{namespace}.%"),  # pylint: disable=no-member
                    Node.namespace == namespace,
                ),
                Node.type == node_type if node_type else True,
            )
            .options(
                load_only(
                    Node.name,
                    Node.type,
                    Node.current_version,
                ),
                joinedload(Node.current).options(
                    load_only(
                        NodeRevision.display_name,
                        NodeRevision.description,
                        NodeRevision.status,
                        NodeRevision.mode,
                        NodeRevision.updated_at,
                    ),
                ),
                selectinload(Node.tags),
                *([selectinload(Node.history)] if with_edited_by else []),
            )
        )
        if include_deactivated is False:
            list_nodes_query = list_nodes_query.where(is_(Node.deactivated_at, None))

        result = await session.execute(list_nodes_query)
        return [
            NodeMinimumDetail(
                name=row.name,
                display_name=row.current.display_name,
                description=row.current.description,
                version=row.current_version,
                type=row.type,
                status=row.current.status,
                mode=row.current.mode,
                updated_at=row.current.updated_at,
                tags=row.tags,
                edited_by=(
                    None
                    if not with_edited_by
                    else list({entry.user for entry in row.history if entry.user})
                ),
            )
            for row in result.unique().scalars().all()
        ]

    @classmethod
    async def list_all_nodes(
        cls,
        session: AsyncSession,
        namespace: str,
        include_deactivated: bool = False,
    ) -> List["Node"]:
        """
        List all nodes in the namespace.
        """
        await cls.get(session, namespace)

        list_nodes_query = (
            select(Node)
            .where(
                or_(
                    Node.namespace.like(f"{namespace}.%"),  # pylint: disable=no-member
                    Node.namespace == namespace,
                ),
            )
            .options(
                *_node_output_options(),
            )
        )
        if include_deactivated is False:  # pragma: no cover
            list_nodes_query = list_nodes_query.where(is_(Node.deactivated_at, None))

        result = await session.execute(list_nodes_query)
        nodes = result.unique().scalars().all()
        return nodes
