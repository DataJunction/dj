"""Namespace database schema."""

from datetime import datetime
from typing import List, Optional

from sqlalchemy import Boolean, DateTime, ForeignKey, String, case, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import (
    Mapped,
    joinedload,
    load_only,
    mapped_column,
    relationship,
    selectinload,
)
from sqlalchemy.sql.operators import is_, or_

from datajunction_server.database.base import Base
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import DJDoesNotExistException
from datajunction_server.models.node import NodeMinimumDetail
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import _node_output_options
from datajunction_server.typing import UTCDatetime


class NodeNamespace(Base):
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

    # Git configuration for branch management
    github_repo_path: Mapped[Optional[str]] = mapped_column(
        String,
        nullable=True,
        default=None,
    )  # e.g., "owner/repo"

    git_branch: Mapped[Optional[str]] = mapped_column(
        String,
        nullable=True,
        default=None,
    )  # e.g., "main" or "feature-x"

    git_path: Mapped[Optional[str]] = mapped_column(
        String,
        nullable=True,
        default=None,
    )  # e.g., "definitions/" - subdirectory within repo

    default_branch: Mapped[Optional[str]] = mapped_column(
        String,
        nullable=True,
        default=None,
    )  # Default branch for git root namespaces (e.g., "main") - used as source when creating new branches

    parent_namespace: Mapped[Optional[str]] = mapped_column(
        ForeignKey("nodenamespace.namespace", ondelete="RESTRICT"),
        nullable=True,
        default=None,
    )  # Links myproject.feature_x -> myproject.main for PR targeting

    parent_namespace_obj: Mapped[Optional["NodeNamespace"]] = relationship(
        "NodeNamespace",
        foreign_keys=[parent_namespace],
        remote_side="NodeNamespace.namespace",
        lazy="select",  # Use select to avoid conflicts with FOR UPDATE queries
    )

    git_only: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=False,
    )  # If True, UI edits are blocked; must edit via git and deploy

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
    async def list_branch_namespaces(
        cls,
        session: AsyncSession,
        parent: str,
        exclude_namespace: Optional[str] = None,
    ) -> List["NodeNamespace"]:
        """
        List the active branch namespaces whose parent is ``parent``.

        Root namespaces resolve to their direct children; branch namespaces resolve
        to their siblings, which is why ``exclude_namespace`` is used to drop self.
        """
        base_filter = cls.parent_namespace == parent
        if exclude_namespace:
            base_filter = base_filter & (cls.namespace != exclude_namespace)
        statement = select(cls).where(base_filter, cls.deactivated_at.is_(None))
        return list((await session.execute(statement)).scalars().all())

    @classmethod
    async def node_counts_by_namespace(
        cls,
        session: AsyncSession,
        root_namespace: str,
    ) -> dict[str, tuple[int, int, datetime]]:
        """
        Aggregate node counts grouped by namespace for ``root_namespace`` and all of
        its descendants, in a single indexed pass.

        ``root_namespace`` is a literal, so ``LIKE root.%`` uses the
        varchar_pattern_ops index on node.namespace rather than a full table scan.
        ``updated_at`` is set only at revision insert time, so the current revision's
        timestamp is the latest per node — no need to scan historical revisions. It's
        never NULL here (non-nullable column, and every emitted group has a row).

        Returns dict[namespace -> (num_nodes, invalid_node_count, last_updated_at)].
        """
        statement = (
            select(
                Node.namespace,
                func.count(Node.id),
                func.sum(case((NodeRevision.status == "invalid", 1), else_=0)),
                func.max(NodeRevision.updated_at),
            )
            .join(
                NodeRevision,
                (NodeRevision.node_id == Node.id)
                & (NodeRevision.version == Node.current_version),
            )
            .where(
                or_(
                    Node.namespace == root_namespace,
                    Node.namespace.like(f"{root_namespace}.%"),
                ),
            )
            .group_by(Node.namespace)
        )
        result = await session.execute(statement)
        return {
            namespace: (num_nodes, invalid_node_count, last_updated_at)
            for namespace, num_nodes, invalid_node_count, last_updated_at in result
        }

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
                    Node.namespace.like(f"{namespace}.%"),
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
        options: Optional[List] = None,
    ) -> List["Node"]:
        """
        List all nodes in the namespace.
        """
        await cls.get(session, namespace)

        list_nodes_query = (
            select(Node)
            .where(
                or_(
                    Node.namespace.like(f"{namespace}.%"),
                    Node.namespace == namespace,
                ),
            )
            .options(
                *(options or _node_output_options()),
            )
        )
        if include_deactivated is False:  # pragma: no cover
            list_nodes_query = list_nodes_query.where(is_(Node.deactivated_at, None))

        result = await session.execute(list_nodes_query)
        nodes = result.unique().scalars().all()
        return nodes
