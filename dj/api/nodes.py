"""
Node related APIs.
"""

import logging
from http import HTTPStatus
from typing import Dict, List, Optional, Tuple, Union

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import joinedload
from sqlmodel import Session, SQLModel, select

from dj.api.helpers import get_database_by_name, get_node_by_name
from dj.construction.extract import extract_dependencies_from_node
from dj.construction.inference import get_type_of_expression
from dj.errors import DJError, DJException, ErrorCode
from dj.models import Database, Table
from dj.models.column import Column, ColumnType
from dj.models.node import (
    AvailabilityState,
    CreateNode,
    CreateSourceNode,
    Node,
    NodeMode,
    NodeRevision,
    NodeRevisionBase,
    NodeStatus,
    NodeType,
    UpdateNode,
)
from dj.models.table import CreateTable
from dj.sql.parsing import ast
from dj.utils import UTCDatetime, get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


class SimpleColumn(SQLModel):
    """
    A simplified column schema, without ID or dimensions.
    """

    name: str
    type: ColumnType


class TableMetadata(SQLModel):
    """
    Output for table information.
    """

    id: Optional[int]
    catalog: Optional[str]
    schema_: Optional[str]
    table: Optional[str]
    database: Optional[Database]


class NodeRevisionMetadata(SQLModel):
    """
    A node with information about columns and if it is a metric.
    """

    id: int = Field(alias="node_revision_id")
    node_id: int
    type: NodeType
    name: str
    display_name: str
    version: str
    description: str = ""
    query: Optional[str] = None
    availability: Optional[AvailabilityState] = None
    columns: List[SimpleColumn]
    tables: List[TableMetadata]
    updated_at: UTCDatetime

    class Config:  # pylint: disable=missing-class-docstring,too-few-public-methods
        allow_population_by_field_name = True


class OutputModel(BaseModel):
    """
    An output model with the ability to flatten fields. When fields are created with
    `Field(flatten=True)`, the field's values will be automatically flattened into the
    parent output model.
    """

    def _iter(self, *args, to_dict: bool = False, **kwargs):
        for dict_key, value in super()._iter(to_dict, *args, **kwargs):
            if to_dict and self.__fields__[dict_key].field_info.extra.get(
                "flatten",
                False,
            ):
                assert isinstance(value, dict)
                for key, val in value.items():
                    yield key, val
            else:
                yield dict_key, value


class NodeMetadata(OutputModel):
    """
    A node that shows the current revision.
    """

    current: NodeRevisionMetadata = Field(flatten=True)
    created_at: UTCDatetime


class NodeWithRevisions(NodeMetadata):
    """
    Output for a reference node with revision history.
    """

    revisions: List[NodeRevisionMetadata]


class NodeValidation(SQLModel):
    """
    A validation of a provided node definition
    """

    message: str
    status: NodeStatus
    node: Node
    node_revision: NodeRevision
    dependencies: List[NodeRevisionMetadata]
    columns: List[Column]


def validate(
    data: Union[NodeRevisionBase, NodeRevision],
    session: Session = Depends(get_session),
) -> Tuple[Node, NodeRevision, Dict[NodeRevision, List[ast.Table]]]:
    """
    Validate a node.
    """

    node = Node(name=data.name, type=data.type)
    node_revision = NodeRevision.parse_obj(data)
    node_revision.node = node

    # Try to parse the node's query and extract dependencies
    try:
        (
            query_ast,
            dependencies_map,
            missing_parents_map,
        ) = extract_dependencies_from_node(
            session=session,
            node=node_revision,
            raise_=False,
        )
    except ValueError as exc:
        raise DJException(message=str(exc)) from exc

    # Only raise on missing parents if the node mode is set to published
    if missing_parents_map and node_revision.mode == NodeMode.PUBLISHED:
        raise DJException(
            errors=[
                DJError(
                    code=ErrorCode.MISSING_PARENT,
                    message="Node definition contains references to nodes that do not exist",
                    debug={"missing_parents": list(missing_parents_map.keys())},
                ),
            ],
        )

    # Add aliases for any unnamed columns and confirm that all column types can be inferred
    query_ast.select.add_aliases_to_unnamed_columns()
    node_revision.columns = [
        Column(name=col.name.name, type=get_type_of_expression(col))  # type: ignore
        for col in query_ast.select.projection
    ]

    node_revision.status = NodeStatus.VALID
    return node, node_revision, dependencies_map


@router.post("/nodes/validate/", response_model=NodeValidation)
def validate_node(
    data: Union[NodeRevisionBase, NodeRevision],
    session: Session = Depends(get_session),
) -> NodeValidation:
    """
    Validate a node.
    """

    if data.type == NodeType.SOURCE:
        raise DJException(message="Source nodes cannot be validated")

    node, node_revision, dependencies_map = validate(data, session)
    return NodeValidation(
        message=f"Node `{node.name}` is valid",
        status=NodeStatus.VALID,
        node=node,
        node_revision=node_revision,
        dependencies=set(dependencies_map.keys()),
        columns=node_revision.columns,
    )


@router.get("/nodes/", response_model=List[NodeMetadata])
def read_nodes(*, session: Session = Depends(get_session)) -> List[NodeMetadata]:
    """
    List the available nodes.
    """
    nodes = session.exec(select(Node).options(joinedload(Node.current))).all()
    return nodes


@router.get("/nodes/{name}/", response_model=NodeMetadata)
def read_node(name: str, *, session: Session = Depends(get_session)) -> NodeMetadata:
    """
    Show the active version of the specified node.
    """
    statement = select(Node).where(Node.name == name).options(joinedload(Node.current))
    node = session.exec(statement).one_or_none()
    if not node:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Node not found: `{name}`",
        )
    return node  # type: ignore


@router.get("/nodes/{name}/revisions/", response_model=List[NodeRevisionMetadata])
def list_node_revisions(
    name: str, *, session: Session = Depends(get_session)
) -> List[NodeRevisionMetadata]:
    """
    List all revisions for the node.
    """
    statement = select(Node).where(
        Node.name == name,
    )
    node = session.exec(statement).one_or_none()
    if not node:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Node not found: `{name}`",
        )
    return node.revisions  # type: ignore


def create_node_revision(
    data: Union[CreateNode],
    session: Session,
) -> NodeRevision:
    """
    Create a non-source node revision.
    """

    node_revision = NodeRevision.parse_obj(data)
    _, node, dependencies_map = validate(node_revision, session)
    new_parents = [node.name for node in dependencies_map]
    parent_refs = session.exec(
        select(Node).where(
            # pylint: disable=no-member
            Node.name.in_(  # type: ignore
                new_parents,
            ),
        ),
    ).all()
    node_revision.parents = parent_refs

    _logger.info(
        "Parent nodes for %s (v%s): %s",
        data.name,
        node_revision.version,
        [p.name for p in node_revision.parents],
    )
    node_revision.columns = node.columns or []
    return node_revision


@router.post("/nodes/", response_model=NodeMetadata)
def create_node(
    data: Union[CreateSourceNode, CreateNode],
    session: Session = Depends(get_session),
) -> NodeMetadata:
    """
    Create a node.
    """
    query = select(Node).where(Node.name == data.name)
    node = session.exec(query).one_or_none()
    if node:
        raise DJException(f"A node with name `{data.name}` already exists.")

    node = Node(name=data.name, type=data.type, current_version=0)

    if data.type == NodeType.SOURCE:
        node_revision = NodeRevision(
            name=data.name,
            description=data.description,
            type=data.type,
            columns=[
                Column(
                    name=column_name,
                    type=ColumnType[column_data["type"]],
                )
                for column_name, column_data in data.columns.items()
            ],
            parents=[],
        )
    else:
        node_revision = create_node_revision(data, session)

    # Point the node to the new node revision.
    node_revision.node = node
    node_revision.version = str(int(node.current_version) + 1)
    node.current_version = node_revision.version

    node_revision.extra_validation()

    session.add(node)
    session.commit()
    session.refresh(node.current)
    return node  # type: ignore


@router.post("/nodes/{name}/columns/{column}/")
def add_dimension_to_node(
    name: str,
    column: str,
    dimension: Optional[str] = None,
    session: Session = Depends(get_session),
) -> JSONResponse:
    """
    Add information to a node column
    """
    if not dimension:
        raise DJException(
            message="A dimension node must be specified",
            http_status_code=400,
        )

    node = get_node_by_name(session=session, name=name)
    dimension_node = get_node_by_name(
        session=session,
        name=dimension,
        node_type=NodeType.DIMENSION,
    )

    target_column = None
    for node_column in node.current.columns:
        if node_column.name == column:
            target_column = node_column
            node_column.dimension = dimension_node
            node_column.dimension_id = dimension_node.id
    if not target_column:
        raise DJException(
            message=f"A column with name `{column}` on node `{name}` does not exist.",
            http_status_code=404,
        )

    session.add(node)
    session.commit()
    session.refresh(node)
    return JSONResponse(
        status_code=200,
        content={
            "message": (
                f"Dimension node {dimension} has been successfully "
                f"linked to column {column} on node {name}"
            ),
        },
    )


@router.post("/nodes/{name}/table/")
def add_table_to_node(
    name: str, data: CreateTable, *, session: Session = Depends(get_session)
) -> JSONResponse:
    """
    Add a table to a node
    """
    node = get_node_by_name(session=session, name=name)
    database = get_database_by_name(session=session, name=data.database_name)
    table = Table(
        catalog=data.catalog,
        schema=data.schema_,
        table=data.table,
        database_id=database.id,
        cost=data.cost,
        columns=[
            Column(name=column.name, type=ColumnType(column.type))
            for column in data.columns
        ],
    )
    for existing_table in node.current.tables:
        if existing_table.identifier() == table.identifier():
            raise DJException(
                message=f"Table {table.identifier()} already exists for node {name}",
                http_status_code=HTTPStatus.CONFLICT,
            )
    session.add(table)
    session.commit()
    session.refresh(table)

    node.current.tables.append(table)
    session.add(node)
    session.commit()
    session.refresh(node)
    return JSONResponse(
        status_code=200,
        content={
            "message": (
                f"Table {data.table} has been successfully linked to node {name}"
            ),
        },
    )


@router.patch("/nodes/{name}/", response_model=NodeMetadata)
def update_node(
    name: str,
    data: Union[UpdateNode],
    *,
    session: Session = Depends(get_session),
) -> NodeMetadata:
    """
    Update a node.
    """

    query = (
        select(Node)
        .where(Node.name == name)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    node = session.exec(query).one_or_none()
    if not node:
        raise DJException(
            message=f"A node with name `{name}` does not exist.",
            http_status_code=404,
        )

    old_revision = node.current
    old_tables = old_revision.tables
    new_revision = NodeRevision(
        name=old_revision.name,
        display_name=(
            old_revision.display_name if not data.display_name else data.display_name
        ),
        description=(
            old_revision.description if not data.description else data.description
        ),
        query=data.query if data.query else old_revision.query,
        type=old_revision.type,
        columns=[
            Column(
                name=column_name,
                type=ColumnType[column_data["type"]],
                dimension_column=column_data.get("dimension"),
            )
            for column_name, column_data in data.columns.items()
        ]
        if data.columns
        else old_revision.columns,
        tables=old_tables,
        parents=[],
        mode=data.mode if data.mode else old_revision.mode,
    )

    # Build the new version and link its parents
    if new_revision.type != NodeType.SOURCE:
        _, validated_node, dependencies_map = validate(new_revision, session)
        new_parents = [n.name for n in dependencies_map]
        parent_refs = session.exec(
            select(Node).where(
                # pylint: disable=no-member
                Node.name.in_(  # type: ignore
                    new_parents,
                ),
            ),
        ).all()
        new_revision.parents = list(parent_refs)

        _logger.info(
            "Parent nodes for %s (v%s): %s",
            new_revision.name,
            new_revision.version,
            [p.name for p in new_revision.parents],
        )
        new_revision.columns = validated_node.columns or []

    # If nothing has changed, do not create the new node revision
    node_change_check = (
        old_revision.type != NodeType.SOURCE
        and old_revision.query == new_revision.query
        and old_revision.description == new_revision.description
        and old_revision.mode == new_revision.mode
        and old_revision.display_name == new_revision.display_name
    )
    source_node_change_check = (
        old_revision.type == NodeType.SOURCE
        and {col.identifier() for col in old_revision.columns}
        == {col.identifier() for col in new_revision.columns}
        and old_revision.description == new_revision.description
        and old_revision.mode == new_revision.mode
        and old_revision.display_name == new_revision.display_name
    )
    if node_change_check or source_node_change_check:
        return node  # type: ignore

    # Point the reference node to the new node revision.
    new_revision.node = node
    new_revision.version = str(int(node.current_version) + 1)
    node.current_version = new_revision.version

    new_revision.extra_validation()

    session.add(node)
    session.commit()
    session.refresh(node.current)
    return node  # type: ignore
