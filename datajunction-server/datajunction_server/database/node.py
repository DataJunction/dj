"""Node database schema."""
from datetime import datetime, timezone
from functools import partial
from http import HTTPStatus
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import sqlalchemy as sa
from pydantic import Extra
from sqlalchemy import JSON
from sqlalchemy import Column as SqlalchemyColumn
from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, joinedload, mapped_column, relationship, selectinload
from sqlalchemy.sql.base import ExecutableOption
from sqlalchemy.sql.operators import is_

from datajunction_server.database.attributetype import ColumnAttribute
from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.base import Base
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.column import Column
from datajunction_server.database.history import History
from datajunction_server.database.materialization import Materialization
from datajunction_server.database.metricmetadata import MetricMetadata
from datajunction_server.database.tag import Tag
from datajunction_server.database.user import User
from datajunction_server.errors import DJInvalidInputException, DJNodeNotFound
from datajunction_server.models.base import labelize
from datajunction_server.models.node import (
    DEFAULT_DRAFT_VERSION,
    BuildCriteria,
    NodeCursor,
    NodeMode,
    NodeStatus,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import PartitionType
from datajunction_server.naming import amenable_name
from datajunction_server.typing import UTCDatetime
from datajunction_server.utils import SEPARATOR

if TYPE_CHECKING:
    from datajunction_server.database.dimensionlink import DimensionLink


class NodeRelationship(Base):  # pylint: disable=too-few-public-methods
    """
    Join table for self-referential many-to-many relationships between nodes.
    """

    __tablename__ = "noderelationship"

    parent_id: Mapped[int] = mapped_column(
        ForeignKey("node.id", name="fk_noderelationship_parent_id_node"),
        primary_key=True,
    )

    # This will default to `latest`, which points to the current version of the node,
    # or it can be a specific version.
    parent_version: Mapped[Optional[str]] = mapped_column(default="latest")

    child_id: Mapped[int] = mapped_column(
        ForeignKey("noderevision.id", name="fk_noderelationship_child_id_noderevision"),
        primary_key=True,
    )


class CubeRelationship(Base):  # pylint: disable=too-few-public-methods
    """
    Join table for many-to-many relationships between cube nodes and metric/dimension nodes.
    """

    __tablename__ = "cube"

    cube_id: Mapped[int] = mapped_column(
        ForeignKey("noderevision.id", name="fk_cube_cube_id_noderevision"),
        primary_key=True,
    )

    cube_element_id: Mapped[int] = mapped_column(
        ForeignKey("column.id", name="fk_cube_cube_element_id_column"),
        primary_key=True,
    )


class BoundDimensionsRelationship(Base):  # pylint: disable=too-few-public-methods
    """
    Join table for many-to-many relationships between metric nodes
    and parent nodes for dimensions that are required.
    """

    __tablename__ = "metric_required_dimensions"

    metric_id: Mapped[int] = mapped_column(
        ForeignKey(
            "noderevision.id",
            name="fk_metric_required_dimensions_metric_id_noderevision",
        ),
        primary_key=True,
    )

    bound_dimension_id: Mapped[int] = mapped_column(
        ForeignKey(
            "column.id",
            name="fk_metric_required_dimensions_bound_dimension_id_column",
        ),
        primary_key=True,
    )


class MissingParent(Base):  # pylint: disable=too-few-public-methods
    """
    A missing parent node
    """

    __tablename__ = "missingparent"

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(String)
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.now, timezone.utc),
    )


class NodeMissingParents(Base):  # pylint: disable=too-few-public-methods
    """
    Join table for missing parents
    """

    __tablename__ = "nodemissingparents"

    missing_parent_id: Mapped[int] = mapped_column(
        ForeignKey(
            "missingparent.id",
            name="fk_nodemissingparents_missing_parent_id_missingparent",
        ),
        primary_key=True,
    )
    referencing_node_id: Mapped[int] = mapped_column(
        ForeignKey(
            "noderevision.id",
            name="fk_nodemissingparents_referencing_node_id_noderevision",
        ),
        primary_key=True,
    )


class Node(Base):  # pylint: disable=too-few-public-methods
    """
    Node that acts as an umbrella for all node revisions
    """

    __tablename__ = "node"
    __table_args__ = (
        UniqueConstraint("name", "namespace", name="unique_node_namespace_name"),
        Index("cursor_index", "created_at", "id", postgresql_using="btree"),
        Index(
            "namespace_index",
            "namespace",
            postgresql_using="btree",
            postgresql_ops={"identifier": "varchar_pattern_ops"},
        ),
    )

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(String, unique=True)
    type: Mapped[NodeType] = mapped_column(Enum(NodeType))
    display_name: Mapped[Optional[str]]
    created_by_id: int = SqlalchemyColumn(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
    )

    created_by: Mapped[User] = relationship(
        "User",
        back_populates="created_nodes",
        foreign_keys=[created_by_id],
        lazy="selectin",
    )
    namespace: Mapped[str] = mapped_column(String, default="default")
    current_version: Mapped[str] = mapped_column(
        String,
        default=str(DEFAULT_DRAFT_VERSION),
    )
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )
    deactivated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )

    revisions: Mapped[List["NodeRevision"]] = relationship(
        "NodeRevision",
        back_populates="node",
        primaryjoin="Node.id==NodeRevision.node_id",
        cascade="all,delete",
        order_by="NodeRevision.updated_at",
    )
    current: Mapped["NodeRevision"] = relationship(
        "NodeRevision",
        primaryjoin=(
            "and_(Node.id==NodeRevision.node_id, "
            "Node.current_version == NodeRevision.version)"
        ),
        viewonly=True,
        uselist=False,
        # lazy="selectin",
        # selectin for one-to-many
        # joined for many-to-many or many-to-one
    )

    children: Mapped[List["NodeRevision"]] = relationship(
        back_populates="parents",
        secondary="noderelationship",
        primaryjoin="Node.id==NodeRelationship.parent_id",
        secondaryjoin="NodeRevision.id==NodeRelationship.child_id",
        order_by="NodeRevision.id",
    )

    tags: Mapped[List["Tag"]] = relationship(
        back_populates="nodes",
        secondary="tagnoderelationship",
        primaryjoin="TagNodeRelationship.node_id==Node.id",
        secondaryjoin="TagNodeRelationship.tag_id==Tag.id",
    )

    missing_table: Mapped[bool] = mapped_column(sa.Boolean, default=False)

    history: Mapped[List[History]] = relationship(
        primaryjoin="History.entity_name==Node.name",
        order_by="History.created_at",
        foreign_keys="History.entity_name",
    )

    def __hash__(self) -> int:
        return hash(self.id)

    @hybrid_property
    def edited_by(self) -> List[str]:
        """
        Editors of the node
        """
        return list(  # pragma: no cover
            {entry.user for entry in self.history if entry.user},
        )

    @classmethod
    async def get_by_name(
        cls,
        session: AsyncSession,
        name: str,
        options: List[ExecutableOption] = None,
        raise_if_not_exists: bool = False,
        include_inactive: bool = False,
        for_update: bool = False,
    ) -> Optional["Node"]:
        """
        Get a node by name
        """
        statement = select(Node).where(Node.name == name)
        options = options or [
            joinedload(Node.current).options(
                *NodeRevision.default_load_options(),
            ),
            selectinload(Node.tags),
            selectinload(Node.created_by),
        ]
        statement = statement.options(*options)
        if not include_inactive:
            statement = statement.where(is_(Node.deactivated_at, None))
        if for_update:
            statement = statement.with_for_update().execution_options(
                populate_existing=True,
            )
        result = await session.execute(statement)
        node = result.unique().scalar_one_or_none()
        if not node and raise_if_not_exists:
            raise DJNodeNotFound(
                message=(f"A node with name `{name}` does not exist."),
                http_status_code=404,
            )
        return node

    @classmethod
    async def get_by_names(
        cls,
        session: AsyncSession,
        names: List[str],
        options: List[ExecutableOption] = None,
        include_inactive: bool = False,
    ) -> List["Node"]:
        """
        Get a node by name
        """
        statement = select(Node).where(Node.name.in_(names))
        options = options or [
            joinedload(Node.current).options(
                *NodeRevision.default_load_options(),
            ),
            selectinload(Node.tags),
        ]
        statement = statement.options(*options)
        if not include_inactive:  # pragma: no cover
            statement = statement.where(is_(Node.deactivated_at, None))
        result = await session.execute(statement)
        nodes = result.unique().scalars().all()
        return nodes

    @classmethod
    async def get_cube_by_name(
        cls,
        session: AsyncSession,
        name: str,
    ) -> Optional["Node"]:
        """
        Get a cube by name
        """
        statement = (
            select(Node)
            .where(Node.name == name)
            .options(
                joinedload(Node.current).options(
                    selectinload(NodeRevision.availability),
                    selectinload(NodeRevision.columns),
                    selectinload(NodeRevision.catalog).selectinload(Catalog.engines),
                    selectinload(NodeRevision.materializations).joinedload(
                        Materialization.backfills,
                    ),
                    selectinload(NodeRevision.cube_elements)
                    .selectinload(Column.node_revisions)
                    .options(
                        selectinload(NodeRevision.node),
                    ),
                ),
                joinedload(Node.tags),
            )
        )
        result = await session.execute(statement)
        node = result.unique().scalar_one_or_none()
        return node

    @classmethod
    async def get_by_id(
        cls,
        session: AsyncSession,
        node_id: int,
        *options: ExecutableOption,
    ) -> Optional["Node"]:
        """
        Get a node by id
        """
        statement = (
            select(Node).where(Node.id == node_id).options(*options)
        )  # pragma: no cover
        result = await session.execute(statement)  # pragma: no cover
        node = result.unique().scalar_one_or_none()  # pragma: no cover
        return node  # pragma: no cover

    @classmethod
    async def find(  # pylint: disable=keyword-arg-before-vararg
        cls,
        session: AsyncSession,
        prefix: Optional[str] = None,
        node_type: Optional[NodeType] = None,
        *options: ExecutableOption,
    ) -> List["Node"]:
        """
        Finds a list of nodes by prefix
        """
        statement = select(Node).where(is_(Node.deactivated_at, None))
        if prefix:
            statement = statement.where(
                Node.name.like(f"{prefix}%"),  # type: ignore  # pylint: disable=no-member
            )
        if node_type:
            statement = statement.where(Node.type == node_type)
        result = await session.execute(statement.options(*options))
        return result.unique().scalars().all()

    @classmethod
    async def find_by(  # pylint: disable=keyword-arg-before-vararg,too-many-locals
        cls,
        session: AsyncSession,
        names: Optional[List[str]] = None,
        fragment: Optional[str] = None,
        node_types: Optional[List[NodeType]] = None,
        tags: Optional[List[str]] = None,
        edited_by: Optional[str] = None,
        namespace: Optional[str] = None,
        limit: Optional[int] = 100,
        before: Optional[str] = None,
        after: Optional[str] = None,
        *options: ExecutableOption,  # pylint: disable=keyword-arg-before-vararg
    ) -> List["Node"]:
        """
        Finds a list of nodes by prefix
        """
        nodes_with_tags = []
        if tags:
            statement = (
                select(Tag).where(Tag.name.in_(tags)).options(joinedload(Tag.nodes))
            )
            nodes_with_tags = [
                node.id
                for tag in (await session.execute(statement)).unique().scalars().all()
                for node in tag.nodes
            ]
            if not nodes_with_tags:  # pragma: no cover
                return []

        statement = select(Node).where(is_(Node.deactivated_at, None))
        if namespace:
            statement = statement.where(
                (Node.namespace.like(f"{namespace}.%")) | (Node.namespace == namespace),
            )
        if nodes_with_tags:
            statement = statement.where(
                Node.id.in_(nodes_with_tags),
            )  # pragma: no cover
        if names:
            statement = statement.where(
                Node.name.in_(names),  # type: ignore  # pylint: disable=no-member
            )
        if fragment:
            statement = statement.where(
                Node.name.like(f"%{fragment}%"),  # type: ignore  # pylint: disable=no-member
            )
        if node_types:
            statement = statement.where(Node.type.in_(node_types))
        if edited_by:
            edited_node_subquery = (
                select(History.entity_name)
                .where((History.user == edited_by))
                .distinct()
                .subquery()
            )

            statement = statement.join(
                edited_node_subquery,
                onclause=(edited_node_subquery.c.entity_name == Node.name),
            ).distinct()

        if after:
            cursor = NodeCursor.decode(after)
            statement = statement.where(
                (Node.created_at, Node.id)
                <= (cursor.created_at, cursor.id),  # pylint: disable=no-member
            ).order_by(Node.created_at.desc(), Node.id.desc())
        elif before:
            cursor = NodeCursor.decode(before)
            statement = statement.where(
                (Node.created_at, Node.id)
                >= (cursor.created_at, cursor.id),  # pylint: disable=no-member
            )
            statement = statement.order_by(Node.created_at.asc(), Node.id.asc())
        else:
            statement = statement.order_by(Node.created_at.desc(), Node.id.desc())

        limit = limit if limit and limit > 0 else 100
        statement = statement.limit(limit)
        result = await session.execute(statement.options(*options))
        nodes = result.unique().scalars().all()

        # Reverse for backward pagination
        if before:
            nodes.reverse()
        return nodes


class NodeRevision(
    Base,
):  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """
    A node revision.
    """

    __tablename__ = "noderevision"
    __table_args__ = (UniqueConstraint("version", "node_id"),)

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )

    name: Mapped[str] = mapped_column(unique=False)

    display_name: Mapped[Optional[str]] = mapped_column(
        String,
        insert_default=lambda context: labelize(context.current_parameters.get("name")),
    )
    type: Mapped[NodeType] = mapped_column(Enum(NodeType))
    description: Mapped[str] = mapped_column(String, default="")
    created_by_id: int = SqlalchemyColumn(
        Integer,
        ForeignKey("users.id"),
        nullable=False,
    )
    created_by: Mapped[User] = relationship(
        "User",
        back_populates="created_node_revisions",
        foreign_keys=[created_by_id],
        lazy="selectin",
    )
    query: Mapped[Optional[str]] = mapped_column(String)
    mode: Mapped[NodeMode] = mapped_column(
        Enum(NodeMode),
        default=NodeMode.PUBLISHED,  # pylint: disable=no-member
    )

    version: Mapped[Optional[str]] = mapped_column(
        String,
        default=str(DEFAULT_DRAFT_VERSION),
    )
    node_id: Mapped[int] = mapped_column(
        ForeignKey("node.id", name="fk_noderevision_node_id_node"),
    )
    node: Mapped[Node] = relationship(
        "Node",
        back_populates="revisions",
        foreign_keys=[node_id],
        lazy="selectin",
    )
    catalog_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("catalog.id", name="fk_noderevision_catalog_id_catalog"),
    )
    catalog: Mapped[Optional[Catalog]] = relationship(
        "Catalog",
        back_populates="node_revisions",
        lazy="joined",
    )
    schema_: Mapped[Optional[str]] = mapped_column(String, default=None)
    table: Mapped[Optional[str]] = mapped_column(String, default=None)

    # A list of columns from the metric's parent that
    # are required for grouping when using the metric
    required_dimensions: Mapped[List["Column"]] = relationship(
        secondary="metric_required_dimensions",
        primaryjoin="NodeRevision.id==BoundDimensionsRelationship.metric_id",
        secondaryjoin="Column.id==BoundDimensionsRelationship.bound_dimension_id",
    )

    metric_metadata_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey(
            "metricmetadata.id",
            name="fk_noderevision_metric_metadata_id_metricmetadata",
        ),
    )
    metric_metadata: Mapped[Optional[MetricMetadata]] = relationship(
        primaryjoin="NodeRevision.metric_metadata_id==MetricMetadata.id",
        cascade="all, delete",
        uselist=False,
    )

    # A list of metric columns and dimension columns, only used by cube nodes
    cube_elements: Mapped[List["Column"]] = relationship(
        secondary="cube",
        primaryjoin="NodeRevision.id==CubeRelationship.cube_id",
        secondaryjoin="Column.id==CubeRelationship.cube_element_id",
        lazy="joined",
        order_by="Column.order",
    )

    status: Mapped[NodeStatus] = mapped_column(
        Enum(NodeStatus),
        default=NodeStatus.INVALID,
    )
    updated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        insert_default=partial(datetime.now, timezone.utc),
    )

    parents: Mapped[List["Node"]] = relationship(
        back_populates="children",
        secondary="noderelationship",
        primaryjoin="NodeRevision.id==NodeRelationship.child_id",
        secondaryjoin="Node.id==NodeRelationship.parent_id",
    )

    missing_parents: Mapped[List[MissingParent]] = relationship(
        secondary="nodemissingparents",
        primaryjoin="NodeRevision.id==NodeMissingParents.referencing_node_id",
        secondaryjoin="MissingParent.id==NodeMissingParents.missing_parent_id",
        cascade="all, delete",
    )

    columns: Mapped[List["Column"]] = relationship(
        secondary="nodecolumns",
        primaryjoin="NodeRevision.id==NodeColumns.node_id",
        secondaryjoin="Column.id==NodeColumns.column_id",
        cascade="all, delete",
        order_by="Column.order",
    )

    dimension_links: Mapped[List["DimensionLink"]] = relationship(
        back_populates="node_revision",
        cascade="all, delete",
        order_by="DimensionLink.id",
    )

    # The availability of materialized data needs to be stored on the NodeRevision
    # level in order to support pinned versions, where a node owner wants to pin
    # to a particular upstream node version.
    availability: Mapped[Optional[AvailabilityState]] = relationship(
        secondary="nodeavailabilitystate",
        primaryjoin="NodeRevision.id==NodeAvailabilityState.node_id",
        secondaryjoin="AvailabilityState.id==NodeAvailabilityState.availability_id",
        cascade="all, delete",
        uselist=False,
    )

    # Nodes of type SOURCE will not have this property as their materialization
    # is not managed as a part of this service
    materializations: Mapped[List["Materialization"]] = relationship(
        back_populates="node_revision",
        cascade="all, delete-orphan",
    )

    lineage: Mapped[Optional[List[Dict]]] = mapped_column(
        JSON,
        default=[],
    )

    def __hash__(self) -> int:
        return hash(self.id)

    def primary_key(self) -> List[Column]:
        """
        Returns the primary key columns of this node.
        """
        primary_key_columns = []
        for col in self.columns:  # pylint: disable=not-an-iterable
            if col.has_primary_key_attribute():
                primary_key_columns.append(col)
        return primary_key_columns

    @classmethod
    def default_load_options(cls):
        """
        Default options when loading a node
        """
        # pylint: disable=import-outside-toplevel
        from datajunction_server.database.dimensionlink import DimensionLink

        return (
            selectinload(NodeRevision.columns).options(
                joinedload(Column.attributes).joinedload(
                    ColumnAttribute.attribute_type,
                ),
                joinedload(Column.dimension),
                joinedload(Column.partition),
            ),
            joinedload(NodeRevision.catalog),
            selectinload(NodeRevision.parents),
            selectinload(NodeRevision.materializations),
            selectinload(NodeRevision.metric_metadata),
            selectinload(NodeRevision.availability),
            selectinload(NodeRevision.dimension_links).options(
                joinedload(DimensionLink.dimension).options(
                    selectinload(Node.current),
                ),
            ),
            selectinload(NodeRevision.required_dimensions),
        )

    @staticmethod
    def format_metric_alias(query: str, name: str) -> str:
        """
        Return a metric query with the metric aliases reassigned to
        have the same name as the node, if they aren't already matching.
        """
        from datajunction_server.sql.parsing import (  # pylint: disable=import-outside-toplevel
            ast,
        )
        from datajunction_server.sql.parsing.backends.antlr4 import (  # pylint: disable=import-outside-toplevel
            parse,
        )

        tree = parse(query)
        projection_0 = tree.select.projection[0]
        tree.select.projection[0] = projection_0.set_alias(
            ast.Name(amenable_name(name)),
        )
        return str(tree)

    def check_metric(self):
        """
        Check if the Node defines a metric.

        The Node SQL query should have a single expression in its
        projections and it should be an aggregation function.
        """
        from datajunction_server.sql.parsing.backends.antlr4 import (  # pylint: disable=import-outside-toplevel
            parse,
        )

        # must have a single expression
        tree = parse(self.query)
        if len(tree.select.projection) != 1:
            raise DJInvalidInputException(
                http_status_code=HTTPStatus.BAD_REQUEST,
                message="Metric queries can only have a single "
                f"expression, found {len(tree.select.projection)}",
            )
        projection_0 = tree.select.projection[0]

        # must have an aggregation
        if (
            not hasattr(projection_0, "is_aggregation")
            or not projection_0.is_aggregation()  # type: ignore
        ):
            raise DJInvalidInputException(
                http_status_code=HTTPStatus.BAD_REQUEST,
                message=f"Metric {self.name} has an invalid query, "
                "should have an aggregate expression",
            )

    def extra_validation(self) -> None:
        """
        Extra validation for node data.
        """
        if self.type in {NodeType.TRANSFORM, NodeType.METRIC, NodeType.DIMENSION}:
            if not self.query:
                raise DJInvalidInputException(
                    f"Node {self.name} of type {self.type} needs a query",
                )

        if self.type != NodeType.METRIC and self.required_dimensions:
            raise DJInvalidInputException(
                f"Node {self.name} of type {self.type} cannot have "
                "bound dimensions which are only for metrics.",
            )

        if self.type == NodeType.METRIC:
            self.check_metric()

        if self.type == NodeType.CUBE:
            if not self.cube_elements:
                raise DJInvalidInputException(
                    f"Node {self.name} of type cube node needs cube elements",
                )

    def copy_dimension_links_from_revision(self, old_revision: "NodeRevision"):
        """
        Copy dimension links and attributes from another node revision if the column names match
        """
        old_columns_mapping = {col.name: col for col in old_revision.columns}
        for col in self.columns:  # pylint: disable=not-an-iterable
            if col.name in old_columns_mapping:
                col.dimension_id = old_columns_mapping[col.name].dimension_id
                col.attributes = old_columns_mapping[col.name].attributes or []
        return self

    class Config:  # pylint: disable=missing-class-docstring,too-few-public-methods
        extra = Extra.allow

    def has_available_materialization(self, build_criteria: BuildCriteria) -> bool:
        """
        Has a materialization available
        """
        return (
            self.availability is not None  # pragma: no cover
            and self.availability.is_available(  # pylint: disable=no-member
                criteria=build_criteria,
            )
        )

    def ordering(self) -> Dict[str, int]:
        """
        Column ordering
        """
        return {
            col.name.replace("_DOT_", SEPARATOR): (col.order or idx)
            for idx, col in enumerate(self.columns)
        }

    def cube_elements_with_nodes(self) -> List[Tuple[Column, Optional["NodeRevision"]]]:
        """
        Cube elements along with their nodes
        """
        return [
            (element, element.node_revision())
            for element in self.cube_elements  # pylint: disable=not-an-iterable
        ]

    def cube_metrics(self) -> List[Node]:
        """
        Cube node's metrics
        """
        if self.type != NodeType.CUBE:
            return []  # pragma: no cover
        ordering = {
            col.name.replace("_DOT_", SEPARATOR): (col.order or idx)
            for idx, col in enumerate(self.columns)
        }
        return sorted(
            [
                node_revision.node  # type: ignore
                for element, node_revision in self.cube_elements_with_nodes()
                if node_revision
                and node_revision.node
                and node_revision.type == NodeType.METRIC
            ],
            key=lambda x: ordering[x.name],
        )

    def cube_dimensions(self) -> List[str]:
        """
        Cube node's dimension attributes
        """
        if self.type != NodeType.CUBE:
            return []  # pragma: no cover
        dimension_to_roles_mapping = {
            col.name: col.dimension_column for col in self.columns
        }
        ordering = {
            (col.name + (col.dimension_column or "")).split("[")[0]: col.order or idx
            for idx, col in enumerate(self.columns)
        }
        return sorted(
            [
                node_revision.name
                + SEPARATOR
                + element.name
                + dimension_to_roles_mapping.get(element.name, "")
                for element, node_revision in self.cube_elements_with_nodes()
                if node_revision and node_revision.type != NodeType.METRIC
            ],
            key=lambda x: ordering[x],
        )

    @hybrid_property
    def cube_node_metrics(self) -> List[str]:
        """
        Cube node's metrics
        """
        return [metric.name for metric in self.cube_metrics()]

    @hybrid_property
    def cube_node_dimensions(self) -> List[str]:
        """
        Cube node's dimension attributes
        """
        return self.cube_dimensions()

    def temporal_partition_columns(self) -> List[Column]:
        """
        The node's temporal partition columns, if any
        """
        return [
            col
            for col in self.columns  # pylint: disable=not-an-iterable
            if col.partition and col.partition.type_ == PartitionType.TEMPORAL
        ]

    def categorical_partition_columns(self) -> List[Column]:
        """
        The node's categorical partition columns, if any
        """
        return [
            col
            for col in self.columns  # pylint: disable=not-an-iterable
            if col.partition and col.partition.type_ == PartitionType.CATEGORICAL
        ]

    def dimensions_to_columns_map(self):
        """
        A mapping between each of the dimension attributes linked to this node to the columns
        that they're linked to.
        """
        return {  # pragma: no cover
            left.identifier(): right
            for link in self.dimension_links
            for left, right in link.foreign_key_mapping().items()
        }

    def __deepcopy__(self, memo):
        """
        Note: We should not use copy or deepcopy to copy any SQLAlchemy objects.
        This is implemented here to make copying of AST structures easier, but does
        not actually copy anything
        """
        return None


class NodeColumns(Base):  # pylint: disable=too-few-public-methods
    """
    Join table for node columns.
    """

    __tablename__ = "nodecolumns"

    node_id: Mapped[int] = mapped_column(  # pylint: disable=unsubscriptable-object
        ForeignKey("noderevision.id", name="fk_nodecolumns_node_id_noderevision"),
        primary_key=True,
    )
    column_id: Mapped[int] = mapped_column(  # pylint: disable=unsubscriptable-object
        ForeignKey("column.id", name="fk_nodecolumns_column_id_column"),
        primary_key=True,
    )
