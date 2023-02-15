"""
Node related APIs.
"""
import http.client
import logging
from http import HTTPStatus
from typing import List, Optional, Union

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import joinedload
from sqlmodel import Session, select

from dj.api.helpers import (
    get_catalog,
    get_column,
    get_database_by_name,
    get_downstream_nodes,
    get_engine,
    get_node_by_name,
    validate_node_data,
)
from dj.errors import DJException
from dj.models import Table
from dj.models.column import Column, ColumnType
from dj.models.node import (
    DEFAULT_DRAFT_VERSION,
    DEFAULT_PUBLISHED_VERSION,
    CreateCubeNode,
    CreateNode,
    CreateSourceNode,
    MaterializationConfig,
    Node,
    NodeMode,
    NodeOutput,
    NodeRevision,
    NodeRevisionBase,
    NodeRevisionOutput,
    NodeStatus,
    NodeType,
    NodeValidation,
    UpdateNode,
    UpsertMaterializationConfig,
)
from dj.models.table import CreateTable
from dj.sql.parsing.backends.sqloxide import parse
from dj.utils import Version, VersionUpgrade, get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


def raise_on_query_not_allowed(
    data: Union[CreateNode, CreateSourceNode, CreateCubeNode],
) -> None:
    """
    Check if the payload includes a query for a node type that can't have one
    """
    if (
        data.type in (NodeType.SOURCE, NodeType.CUBE)
        and not isinstance(data, (CreateSourceNode, CreateCubeNode))
        and data.query
    ):
        raise DJException(
            message=(f"Query not allowed for node of type {data.type}"),
            http_status_code=http.client.UNPROCESSABLE_ENTITY,
        )


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

    node, node_revision, dependencies_map = validate_node_data(data, session)
    return NodeValidation(
        message=f"Node `{node.name}` is valid",
        status=NodeStatus.VALID,
        node=node,
        node_revision=node_revision,
        dependencies=set(dependencies_map.keys()),
        columns=node_revision.columns,
    )


@router.get("/nodes/", response_model=List[NodeOutput])
def read_nodes(*, session: Session = Depends(get_session)) -> List[NodeOutput]:
    """
    List the available nodes.
    """
    nodes = session.exec(select(Node).options(joinedload(Node.current))).unique().all()
    return nodes


@router.get("/nodes/{name}/", response_model=NodeOutput)
def read_node(name: str, *, session: Session = Depends(get_session)) -> NodeOutput:
    """
    Show the active version of the specified node.
    """
    node = get_node_by_name(session, name, with_current=True)
    return node  # type: ignore


@router.post("/nodes/{name}/materialization/")
def upsert_node_materialization_config(
    name: str,
    data: UpsertMaterializationConfig,
    *,
    session: Session = Depends(get_session),
) -> JSONResponse:
    """
    Update materialization config of the specified node.
    """
    node = get_node_by_name(session, name, with_current=True)
    if node.type == NodeType.SOURCE:
        raise DJException(
            http_status_code=HTTPStatus.BAD_REQUEST,
            message=f"Cannot set materialization config for source node `{name}`!",
        )
    current_revision = node.current

    # Check to see if a config for this engine already exists with the exact same config
    existing_config_for_engine = [
        config
        for config in node.current.materialization_configs
        if config.engine.name == data.engine_name
    ]
    if (
        existing_config_for_engine
        and existing_config_for_engine[0].config == data.config
    ):
        return JSONResponse(
            status_code=HTTPStatus.NO_CONTENT,
            content={
                "message": (
                    f"The same materialization config provided already exists for "
                    f"node `{name}` so no update was performed."
                ),
            },
        )

    # Materialization config changed, so create a new materialization config and a new node
    # revision that references it.
    engine = get_engine(session, data.engine_name, data.engine_version)
    new_node_revision = create_new_revision_from_existing(
        session,
        current_revision,
        node,
        version_upgrade=VersionUpgrade.MAJOR,
    )

    unchanged_existing_configs = [
        config
        for config in node.current.materialization_configs
        if config.engine.name != data.engine_name
    ]
    new_config = MaterializationConfig(
        node_revision=new_node_revision,
        engine=engine,
        config=data.config,
    )
    new_node_revision.materialization_configs = unchanged_existing_configs + [  # type: ignore
        new_config,
    ]
    node.current_version = new_node_revision.version  # type: ignore

    # This will add the materialization config, the new node rev, and update the node's version.
    session.add(new_node_revision)
    session.add(node)
    session.commit()

    return JSONResponse(
        status_code=200,
        content={
            "message": (
                f"Successfully updated materialization config for node `{name}`"
                f" and engine `{engine.name}`."
            ),
        },
    )


@router.get("/nodes/{name}/revisions/", response_model=List[NodeRevisionOutput])
def list_node_revisions(
    name: str, *, session: Session = Depends(get_session)
) -> List[NodeRevisionOutput]:
    """
    List all revisions for the node.
    """
    node = get_node_by_name(session, name, with_current=False)
    return node.revisions  # type: ignore


def create_node_revision(
    data: Union[CreateNode],
    session: Session,
) -> NodeRevision:
    """
    Create a non-source node revision.
    """

    node_revision = NodeRevision.parse_obj(data)
    _, node, dependencies_map = validate_node_data(node_revision, session)
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
        "Parent nodes for %s (%s): %s",
        data.name,
        node_revision.version,
        [p.name for p in node_revision.parents],
    )
    node_revision.columns = node.columns or []
    return node_revision


def create_cube_node_revision(
    session: Session,
    data: Union[CreateCubeNode],
) -> NodeRevision:
    """
    Create a cube node revision.
    """
    if not data.cube_elements:
        raise DJException(
            message=("Cannot create a cube node with no cube elements"),
            http_status_code=http.client.UNPROCESSABLE_ENTITY,
        )
    metrics = []
    dimensions = []
    for node_name in data.cube_elements:
        cube_element = get_node_by_name(session=session, name=node_name)
        if cube_element.type == NodeType.METRIC:
            metrics.append(cube_element)
        elif cube_element.type == NodeType.DIMENSION:
            dimensions.append(cube_element)
        else:
            raise DJException(
                message=(
                    f"Node {cube_element.name} of type {cube_element.type} "
                    "cannot be added to a cube"
                ),
                http_status_code=http.client.UNPROCESSABLE_ENTITY,
            )
    if not metrics:
        raise DJException(
            message=("At least one metric is required to create a cube node"),
            http_status_code=http.client.UNPROCESSABLE_ENTITY,
        )
    if not dimensions:
        raise DJException(
            message=("At least one dimension is required to create a cube node"),
            http_status_code=http.client.UNPROCESSABLE_ENTITY,
        )
    return NodeRevision(
        name=data.name,
        description=data.description,
        type=data.type,
        cube_elements=metrics + dimensions,
    )


@router.post("/nodes/", response_model=NodeOutput)
def create_node(
    data: Union[CreateSourceNode, CreateCubeNode, CreateNode],
    session: Session = Depends(get_session),
) -> NodeOutput:
    """
    Create a node.
    """
    query = select(Node).where(Node.name == data.name)
    node = session.exec(query).one_or_none()
    if node:
        raise DJException(
            message=f"A node with name `{data.name}` already exists.",
            http_status_code=HTTPStatus.CONFLICT,
        )

    node = Node(name=data.name, type=data.type, current_version=0)

    raise_on_query_not_allowed(data)

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
    elif data.type == NodeType.CUBE:
        node_revision = create_cube_node_revision(session=session, data=data)
    else:
        node_revision = create_node_revision(data, session)

    # Point the node to the new node revision.
    node_revision.node = node
    node_revision.version = (
        str(DEFAULT_DRAFT_VERSION)
        if data.mode == NodeMode.DRAFT
        else str(DEFAULT_PUBLISHED_VERSION)
    )
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
    dimension_column: Optional[str] = None,
    session: Session = Depends(get_session),
) -> JSONResponse:
    """
    Add information to a node column
    """
    if not dimension:  # If no dimension is set, assume it matches the column name
        dimension = column

    node = get_node_by_name(session=session, name=name)
    dimension_node = get_node_by_name(
        session=session,
        name=dimension,
        node_type=NodeType.DIMENSION,
    )

    if dimension_column:  # Check that the column exists before linking
        get_column(dimension_node.current, dimension_column)

    target_column = get_column(node.current, column)
    target_column.dimension = dimension_node
    target_column.dimension_id = dimension_node.id
    target_column.dimension_column = dimension_column

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
    catalog = get_catalog(session=session, name=data.catalog_name)
    for existing_table in node.current.tables:
        if (
            existing_table.database == database
            and existing_table.catalog == catalog
            and existing_table.table == data.table
        ):
            raise DJException(
                message=(
                    f"Table {data.table} in database {database.name} in "
                    f"catalog {catalog.name} already exists for node {name}"
                ),
                http_status_code=HTTPStatus.CONFLICT,
            )
    table = Table(
        catalog_id=catalog.id,
        schema=data.schema_,
        table=data.table,
        database_id=database.id,
        cost=data.cost,
        columns=[
            Column(name=column.name, type=ColumnType(column.type))
            for column in data.columns
        ],
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


def create_new_revision_from_existing(
    session: Session,
    old_revision: NodeRevision,
    node: Node,
    data: UpdateNode = None,
    version_upgrade: VersionUpgrade = None,
) -> Optional[NodeRevision]:
    """
    Creates a new revision from an existing node revision.
    """
    minor_changes = (
        (data and data.description and old_revision.description != data.description)
        or (data and data.mode and old_revision.mode != data.mode)
        or (
            data
            and data.display_name
            and old_revision.display_name != data.display_name
        )
    )
    query_changes = (
        old_revision.type != NodeType.SOURCE
        and data
        and data.query
        and old_revision.query != data.query
    )
    column_changes = (
        old_revision.type == NodeType.SOURCE
        and data is not None
        and data.columns is not None
        and ({col.identifier() for col in old_revision.columns} != data.columns)
    )
    major_changes = query_changes or column_changes

    # If nothing has changed, do not create the new node revision
    if not minor_changes and not major_changes and not version_upgrade:
        return None

    old_version = Version.parse(node.current_version)
    new_revision = NodeRevision(
        name=old_revision.name,
        node_id=node.id,
        version=str(
            old_version.next_major_version()
            if major_changes or version_upgrade == VersionUpgrade.MAJOR
            else old_version.next_minor_version(),
        ),
        display_name=(
            data.display_name
            if data and data.display_name
            else old_revision.display_name
        ),
        description=(
            data.description if data and data.description else old_revision.description
        ),
        query=(data.query if data and data.query else old_revision.query),
        type=old_revision.type,
        columns=[
            Column(
                name=column_name,
                type=ColumnType[column_data["type"]],
                dimension_column=column_data.get("dimension"),
            )
            for column_name, column_data in data.columns.items()
        ]
        if data and data.columns
        else old_revision.columns,
        tables=old_revision.tables,
        parents=[],
        mode=data.mode if data and data.mode else old_revision.mode,
        materialization_configs=old_revision.materialization_configs,
    )

    # Link the new revision to its parents if the query has changed
    if (
        new_revision.type != NodeType.SOURCE
        and new_revision.query != old_revision.query
    ):
        _, validated_node, dependencies_map = validate_node_data(new_revision, session)
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
    return new_revision


@router.patch("/nodes/{name}/", response_model=NodeOutput)
def update_node(
    name: str,
    data: Union[UpdateNode],
    *,
    session: Session = Depends(get_session),
) -> NodeOutput:
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
    new_revision = create_new_revision_from_existing(session, old_revision, node, data)

    if not new_revision:
        return node  # type: ignore

    node.current_version = new_revision.version

    new_revision.extra_validation()

    session.add(new_revision)
    session.add(node)
    session.commit()
    session.refresh(node.current)
    return node  # type: ignore


@router.get("/nodes/similarity/{node1_name}/{node2_name}")
def node_similarity(
    node1_name: str, node2_name: str, *, session: Session = Depends(get_session)
) -> JSONResponse:
    """
    Compare two nodes by how similar their queries are
    """
    node1 = get_node_by_name(session=session, name=node1_name)
    node2 = get_node_by_name(session=session, name=node2_name)
    if NodeType.SOURCE in (node1.type, node2.type):
        raise DJException(
            message="Cannot determine similarity of source nodes",
            http_status_code=HTTPStatus.CONFLICT,
        )
    node1_ast = parse(node1.current.query)  # type: ignore
    node2_ast = parse(node2.current.query)  # type: ignore
    similarity = node1_ast.similarity_score(node2_ast)
    return JSONResponse(status_code=200, content={"similarity": similarity})


@router.get("/nodes/{name}/downstream/", response_model=List[NodeOutput])
def downstream_nodes(
    name: str, *, node_type: NodeType = None, session: Session = Depends(get_session)
) -> List[NodeOutput]:
    """
    List all nodes that are downstream from the given node, filterable by type.
    """
    return get_downstream_nodes(session, name, node_type)  # type: ignore
