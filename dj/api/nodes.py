# pylint: disable=too-many-lines
"""
Node related APIs.
"""
import logging
import os
from collections import defaultdict
from http import HTTPStatus
from typing import Dict, List, Optional, Set, Union

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import joinedload
from sqlmodel import Session, select
from starlette.requests import Request
from starlette.responses import Response

from dj.api.helpers import (
    get_attribute_type,
    get_catalog,
    get_column,
    get_downstream_nodes,
    get_engine,
    get_node_by_name,
    get_node_namespace,
    get_upstream_nodes,
    propagate_valid_status,
    raise_if_node_exists,
    resolve_downstream_references,
    validate_cube,
    validate_node_data,
)
from dj.api.tags import get_tag_by_name
from dj.construction.build import build_metric_nodes
from dj.errors import DJDoesNotExistException, DJException, DJInvalidInputException
from dj.models import ColumnAttribute
from dj.models.attribute import AttributeType, UniquenessScope
from dj.models.base import generate_display_name
from dj.models.column import Column, ColumnAttributeInput
from dj.models.cube import Measure
from dj.models.node import (
    DEFAULT_DRAFT_VERSION,
    DEFAULT_PUBLISHED_VERSION,
    ColumnOutput,
    CreateCubeNode,
    CreateNode,
    CreateSourceNode,
    MaterializationConfig,
    MissingParent,
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
from dj.service_clients import QueryServiceClient
from dj.sql.parsing import ast
from dj.sql.parsing.backends.antlr4 import parse
from dj.sql.parsing.backends.exceptions import DJParseException
from dj.utils import (
    Version,
    VersionUpgrade,
    get_namespace_from_name,
    get_query_service_client,
    get_session,
)

_logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/nodes/validate/", response_model=NodeValidation)
def validate_a_node(
    data: Union[NodeRevisionBase, NodeRevision],
    session: Session = Depends(get_session),
) -> NodeValidation:
    """
    Validate a node.
    """

    if data.type == NodeType.SOURCE:
        raise DJException(message="Source nodes cannot be validated")

    (
        validated_node,
        dependencies_map,
        missing_parents_map,
        type_inference_failed_columns,
    ) = validate_node_data(data, session)
    if missing_parents_map or type_inference_failed_columns:
        status = NodeStatus.INVALID
    else:
        status = NodeStatus.VALID

    return NodeValidation(
        message=f"Node `{validated_node.name}` is {status}",
        status=status,
        node_revision=validated_node,
        dependencies=set(dependencies_map.keys()),
        columns=validated_node.columns,
    )


def validate_and_build_attribute(
    session: Session,
    attribute_input: ColumnAttributeInput,
    node: Node,
) -> ColumnAttribute:
    """
    Run some validation and build column attribute.
    """
    column_map = {column.name: column for column in node.current.columns}
    if attribute_input.column_name not in column_map:
        raise DJDoesNotExistException(
            message=f"Column `{attribute_input.column_name}` "
            f"does not exist on node `{node.name}`!",
        )
    column = column_map[attribute_input.column_name]
    existing_attributes = {attr.attribute_type.name: attr for attr in column.attributes}
    if attribute_input.attribute_type_name in existing_attributes:
        return existing_attributes[attribute_input.attribute_type_name]

    # Verify attribute type exists
    attribute_type = get_attribute_type(
        session,
        attribute_input.attribute_type_name,
        attribute_input.attribute_type_namespace,
    )
    if not attribute_type:
        raise DJDoesNotExistException(
            message=f"Attribute type `{attribute_input.attribute_type_namespace}"
            f".{attribute_input.attribute_type_name}` "
            f"does not exist!",
        )

    # Verify that the attribute type is allowed for this node
    if node.type not in attribute_type.allowed_node_types:
        raise DJException(
            message=f"Attribute type `{attribute_input.attribute_type_namespace}."
            f"{attribute_type.name}` not allowed on node "
            f"type `{node.type}`!",
        )

    return ColumnAttribute(
        attribute_type=attribute_type,
        column=column,
    )


def set_column_attributes_on_node(
    session: Session,
    attributes: List[ColumnAttributeInput],
    node: Node,
) -> List[Column]:
    """
    Sets the column attributes on the node if allowed.
    """
    modified_columns_map = {}
    for attribute_input in attributes:
        new_attribute = validate_and_build_attribute(session, attribute_input, node)
        # pylint: disable=no-member
        modified_columns_map[new_attribute.column.name] = new_attribute.column

    # Validate column attributes by building mapping between
    # attribute scope and columns
    attributes_columns_map = defaultdict(set)
    modified_columns = modified_columns_map.values()

    for column in modified_columns:
        for attribute in column.attributes:
            scopes_map = {
                UniquenessScope.NODE: attribute.attribute_type,
                UniquenessScope.COLUMN_TYPE: column.type,
            }
            attributes_columns_map[
                (  # type: ignore
                    attribute.attribute_type,
                    tuple(
                        scopes_map[item]
                        for item in attribute.attribute_type.uniqueness_scope
                    ),
                )
            ].add(column.name)

    for (attribute, _), columns in attributes_columns_map.items():
        if len(columns) > 1 and attribute.uniqueness_scope:
            for col in columns:
                modified_columns_map[col].attributes = []
            raise DJException(
                message=f"The column attribute `{attribute.name}` is scoped to be "
                f"unique to the `{attribute.uniqueness_scope}` level, but there "
                "is more than one column tagged with it: "
                f"`{', '.join(sorted(list(columns)))}`",
            )

    session.add_all(modified_columns)
    session.commit()
    for col in modified_columns:
        session.refresh(col)

    session.refresh(node)
    session.refresh(node.current)
    return list(modified_columns)


@router.post(
    "/nodes/{node_name}/attributes/",
    response_model=List[ColumnOutput],
    status_code=201,
)
def set_column_attributes(
    node_name: str,
    attributes: List[ColumnAttributeInput],
    *,
    session: Session = Depends(get_session),
) -> List[ColumnOutput]:
    """
    Set column attributes for the node.
    """
    node = get_node_by_name(session, node_name)
    modified_columns = set_column_attributes_on_node(session, attributes, node)
    return list(modified_columns)  # type: ignore


@router.get("/nodes/", response_model=List[NodeOutput])
def list_nodes(*, session: Session = Depends(get_session)) -> List[NodeOutput]:
    """
    List the available nodes.
    """
    nodes = session.exec(select(Node).options(joinedload(Node.current))).unique().all()
    return nodes


@router.get("/nodes/{name}/", response_model=NodeOutput)
def get_a_node(name: str, *, session: Session = Depends(get_session)) -> NodeOutput:
    """
    Show the active version of the specified node.
    """
    node = get_node_by_name(session, name, with_current=True)
    return node  # type: ignore


@router.delete("/nodes/{name}/", status_code=204)
def delete_a_node(name: str, *, session: Session = Depends(get_session)):
    """
    Delete the specified node.
    """
    node = get_node_by_name(session, name, with_current=True)

    # Find all downstream nodes and mark them as invalid
    downstreams = get_downstream_nodes(session, node.name)
    for downstream in downstreams:
        downstream.current.status = NodeStatus.INVALID
        session.add(downstream)

    # If the node is a dimension, find all columns that
    # are linked to this dimension and remove the link
    if node.type == NodeType.DIMENSION:
        columns = (
            session.exec(select(Column).where(Column.dimension_id == node.id))
            .unique()
            .all()
        )
        for col in columns:
            col.dimension_id = None
            col.dimension_column = None
            session.add(col)
    session.delete(node)
    session.commit()
    return Response(status_code=HTTPStatus.NO_CONTENT.value)


def cube_materialization_config(cube_node: NodeRevision, config: Dict):
    """
    Builds the materialization config for a cube. This includes two parts:
    (a) building a query that decomposes each of the cube's metrics into their
    constituent measures that have simple aggregations
    (b) adding a metric to measures mapping that tells us which measures
    in the query map to each selected metric

    The query in the materialization config is different from the one stored
    on the cube node itself in that this one is meant to create a temporary
    table in preparation for ingestion into an OLAP database like Druid. The
    metric to measures mapping then provides information on how to assemble
    metrics from measures based on this query's output.

    The query directly on the cube node is meant for direct querying of the cube
    without materialization to an OLAP database.
    """
    combined_ast = parse(cube_node.query)
    dimensions_set = {
        dim.name for dim in cube_node.columns if dim.has_dimension_attribute()
    }
    metrics_to_measures = decompose_metrics(combined_ast, dimensions_set)
    new_select_projection: Set[Union[ast.Aliasable, ast.Expression]] = set()
    for expr in combined_ast.select.projection:
        if expr in metrics_to_measures:
            new_select_projection = set(new_select_projection).union(
                metrics_to_measures[expr],
            )
        else:
            new_select_projection.add(expr)
    combined_ast.select.projection = list(new_select_projection)

    config = {
        **config,
        **{
            "measures": {
                metric.alias_or_name.name: sorted(
                    [
                        measure_obj.dict()
                        for measure_obj in {
                            Measure(
                                name=str(measure.alias_or_name),
                                agg=str(measure.child.name),
                                expr=str(measure.child),
                            )
                            for measure in measures
                        }
                    ],
                    key=lambda x: x["name"],
                )
                for metric, measures in metrics_to_measures.items()
            },
        },
        **{"query": str(combined_ast)},
    }
    return config


@router.post("/nodes/{name}/materialization/", status_code=201)
def upsert_a_materialization_config(
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
        for config in current_revision.materialization_configs
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

    # Materialization config changed, so create a new materialization config
    engine = get_engine(session, data.engine_name, data.engine_version)

    unchanged_existing_configs = [
        config
        for config in current_revision.materialization_configs
        if config.engine.name != data.engine_name
    ]

    if current_revision.type == NodeType.CUBE:
        data.config = cube_materialization_config(current_revision, data.config)

    new_config = MaterializationConfig(
        node_revision=current_revision,
        engine=engine,
        config=data.config,
        schedule=data.schedule,
    )
    current_revision.materialization_configs = unchanged_existing_configs + [  # type: ignore
        new_config,
    ]

    # This will add the materialization config, the new node rev, and update the node's version.
    session.add(current_revision)
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
    data: CreateNode,
    node_type: NodeType,
    session: Session,
) -> NodeRevision:
    """
    Create a non-source node revision.
    """
    node_revision = NodeRevision(
        name=data.name,
        namespace=data.namespace,
        display_name=data.display_name
        if data.display_name
        else generate_display_name(data.name),
        description=data.description,
        type=node_type,
        status=NodeStatus.VALID,
        query=data.query,
        mode=data.mode,
    )
    (
        validated_node,
        dependencies_map,
        missing_parents_map,
        type_inference_failed_columns,
    ) = validate_node_data(node_revision, session)
    if missing_parents_map or type_inference_failed_columns:
        node_revision.status = NodeStatus.INVALID
    else:
        node_revision.status = NodeStatus.VALID
    node_revision.missing_parents = [
        MissingParent(name=missing_parent) for missing_parent in missing_parents_map
    ]
    new_parents = [node.name for node in dependencies_map]
    catalog_ids = [node.catalog_id for node in dependencies_map]
    if node_revision.mode == NodeMode.PUBLISHED and not len(set(catalog_ids)) == 1:
        raise DJException(
            f"Cannot create nodes with multi-catalog dependencies: {set(catalog_ids)}",
        )
    catalog_id = next(iter(catalog_ids), 0)
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
    node_revision.columns = validated_node.columns or []
    node_revision.catalog_id = catalog_id
    return node_revision


def decompose_expression(expr: Union[ast.Aliasable, ast.Expression]) -> List[ast.Alias]:
    """
    Simple aggregations are operations that can be computed incrementally as new
    data is ingested, without relying on the results of other aggregations.
    Examples include SUM, COUNT, MIN, MAX.

    Some complex aggregations can be decomposed to simple aggregations: i.e., AVG(x) can
    be decomposed to SUM(x)/COUNT(x).
    """
    if isinstance(expr, ast.Alias):
        expr = expr.child

    simple_aggregations = {"sum", "count", "min", "max"}
    if isinstance(expr, ast.Function):
        function_name = expr.alias_or_name.name.lower()
        columns = [col for arg in expr.args for col in arg.find_all(ast.Column)]
        readable_name = (
            "_".join(
                str(col.alias_or_name).rsplit(".", maxsplit=1)[-1] for col in columns
            )
            if columns
            else "placeholder"
        )
        if function_name in simple_aggregations:
            return [expr.set_alias(ast.Name(f"{readable_name}_{function_name}"))]
        if function_name == "avg":  # pragma: no cover
            return [
                (
                    ast.Function(ast.Name("sum"), args=expr.args).set_alias(
                        ast.Name(f"{readable_name}_sum"),
                    )
                ),
                (
                    ast.Function(ast.Name("count"), args=expr.args).set_alias(
                        ast.Name(f"{readable_name}_count"),
                    )
                ),
            ]
    acceptable_binary_ops = {
        ast.BinaryOpKind.Plus,
        ast.BinaryOpKind.Minus,
        ast.BinaryOpKind.Multiply,
        ast.BinaryOpKind.Divide,
    }
    if isinstance(expr, ast.BinaryOp):
        if expr.op in acceptable_binary_ops:  # pragma: no cover
            measures_left = decompose_expression(expr.left)
            measures_right = decompose_expression(expr.right)
            return measures_left + measures_right
    if isinstance(expr, ast.Cast):
        return decompose_expression(expr.expression)
    raise DJInvalidInputException(  # pragma: no cover
        f"Metric expression {expr} cannot be decomposed into its constituent measures",
    )


def decompose_metrics(combined_ast: ast.Query, dimensions_set: Set[str]):
    """
    Decompose each metric into simple constituent measures and return a dict
    that maps each metric to its measures.
    """
    metrics_to_measures = {}
    for expr in combined_ast.select.projection:
        if expr.alias_or_name.name not in dimensions_set:  # type: ignore
            measure_expressions = decompose_expression(expr)
            metrics_to_measures[expr] = measure_expressions
    return metrics_to_measures


def create_cube_node_revision(  # pylint: disable=too-many-locals
    session: Session,
    data: CreateCubeNode,
) -> NodeRevision:
    """
    Create a cube node revision.
    """
    metric_columns, metric_nodes, dimension_nodes, dimension_columns = validate_cube(
        session,
        data.metrics,
        data.dimensions,
    )

    combined_ast = build_metric_nodes(
        session,
        metric_nodes,
        filters=data.filters or [],
        dimensions=data.dimensions or [],
    )
    dimension_attribute = session.exec(
        select(AttributeType).where(AttributeType.name == "dimension"),
    ).one()
    dimensions_set = {dim.rsplit(".", 1)[1] for dim in data.dimensions}

    node_columns = []
    status = NodeStatus.VALID
    type_inference_failed_columns = []
    for col in combined_ast.select.projection:
        try:
            column_type = col.type  # type: ignore
            column_attributes = (
                [ColumnAttribute(attribute_type=dimension_attribute)]
                if col.alias_or_name.name in dimensions_set
                else []
            )
            node_columns.append(
                Column(
                    name=col.alias_or_name.name,
                    type=column_type,
                    attributes=column_attributes,
                ),
            )
        except DJParseException:  # pragma: no cover
            type_inference_failed_columns.append(col.alias_or_name.name)  # type: ignore
            status = NodeStatus.INVALID

    return NodeRevision(
        name=data.name,
        namespace=data.namespace,
        description=data.description,
        type=NodeType.CUBE,
        query=str(combined_ast),
        columns=node_columns,
        cube_elements=metric_columns + dimension_columns,
        parents=list(set(dimension_nodes + metric_nodes)),
        status=status,
    )


def save_node(
    session: Session,
    node_revision: NodeRevision,
    node: Node,
    node_mode: NodeMode,
):
    """
    Links the node and node revision together and saves them
    """
    node_revision.node = node
    node_revision.version = (
        str(DEFAULT_DRAFT_VERSION)
        if node_mode == NodeMode.DRAFT
        else str(DEFAULT_PUBLISHED_VERSION)
    )
    node.current_version = node_revision.version
    node_revision.extra_validation()

    session.add(node)
    session.commit()

    newly_valid_nodes = resolve_downstream_references(
        session=session,
        node_revision=node_revision,
    )
    propagate_valid_status(
        session=session,
        valid_nodes=newly_valid_nodes,
        catalog_id=node.current.catalog_id,  # pylint: disable=no-member
    )
    session.refresh(node.current)


@router.post("/nodes/source/", response_model=NodeOutput, status_code=201)
def create_a_source(
    data: CreateSourceNode,
    session: Session = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
) -> NodeOutput:
    """
    Create a source node. If columns are not provided, the source node's schema
    will be inferred using the configured query service.
    """
    raise_if_node_exists(session, data.name)

    # Extract and assign namespace if one exists
    namespace = get_namespace_from_name(data.name)
    get_node_namespace(
        session=session,
        namespace=namespace,
    )  # Will return 404 if namespace doesn't exist
    data.namespace = namespace

    node = Node(
        name=data.name,
        namespace=data.namespace,
        type=NodeType.SOURCE,
        current_version=0,
    )
    catalog = get_catalog(session=session, name=data.catalog)

    # When no columns are provided, attempt to find actual table columns
    # if a query service is set
    columns = (
        [
            Column(
                name=column_data.name,
                type=column_data.type,
                dimension=(
                    get_node_by_name(
                        session,
                        name=column_data.dimension,
                        node_type=NodeType.DIMENSION,
                        raise_if_not_exists=False,
                    )
                ),
            )
            for column_data in data.columns
        ]
        if data.columns
        else None
    )
    if not columns:
        if not query_service_client:
            raise DJException(
                message="No table columns were provided and no query "
                "service is configured for table columns inference!",
            )
        columns = query_service_client.get_columns_for_table(
            data.catalog,
            data.schema_,  # type: ignore
            data.table,
            catalog.engines[0] if len(catalog.engines) >= 1 else None,
        )

    node_revision = NodeRevision(
        name=data.name,
        namespace=data.namespace,
        display_name=data.display_name
        if data.display_name
        else generate_display_name(data.name),
        description=data.description,
        type=NodeType.SOURCE,
        status=NodeStatus.VALID,
        catalog_id=catalog.id,
        schema_=data.schema_,
        table=data.table,
        columns=columns,
        parents=[],
    )

    # Point the node to the new node revision.
    save_node(session, node_revision, node, data.mode)
    return node  # type: ignore


@router.post("/nodes/transform/", response_model=NodeOutput, status_code=201)
@router.post("/nodes/dimension/", response_model=NodeOutput, status_code=201)
@router.post("/nodes/metric/", response_model=NodeOutput, status_code=201)
def create_a_node(
    data: CreateNode,
    request: Request,
    *,
    session: Session = Depends(get_session),
) -> NodeOutput:
    """
    Create a node.
    """
    node_type = NodeType(os.path.basename(os.path.normpath(request.url.path)))

    if node_type == NodeType.DIMENSION and not data.primary_key:
        raise DJInvalidInputException("Dimension nodes must define a primary key!")

    raise_if_node_exists(session, data.name)

    namespace = get_namespace_from_name(data.name)
    get_node_namespace(
        session=session,
        namespace=namespace,
    )  # Will return 404 if namespace doesn't exist
    data.namespace = namespace

    node = Node(
        name=data.name,
        namespace=data.namespace,
        type=NodeType(node_type),
        current_version=0,
    )
    node_revision = create_node_revision(data, node_type, session)
    save_node(session, node_revision, node, data.mode)
    session.refresh(node)

    column_names = {col.name for col in node_revision.columns}
    if data.primary_key and any(
        key_column not in column_names for key_column in data.primary_key
    ):
        raise DJInvalidInputException(
            f"Some columns in the primary key {','.join(data.primary_key)} "
            f"were not found in the list of available columns for the node {node.name}.",
        )
    if data.primary_key:
        attributes = [
            ColumnAttributeInput(
                attribute_type_namespace="system",
                attribute_type_name="primary_key",
                column_name=key_column,
            )
            for key_column in data.primary_key
            if key_column in column_names
        ]
        set_column_attributes_on_node(session, attributes, node)
    session.refresh(node)
    session.refresh(node.current)
    return node  # type: ignore


@router.post("/nodes/cube/", response_model=NodeOutput, status_code=201)
def create_a_cube(
    data: CreateCubeNode,
    session: Session = Depends(get_session),
) -> NodeOutput:
    """
    Create a cube node.
    """
    raise_if_node_exists(session, data.name)

    # Extract and assign namespace if one exists
    namespace = get_namespace_from_name(data.name)
    get_node_namespace(
        session=session,
        namespace=namespace,
    )
    data.namespace = namespace

    node = Node(
        name=data.name,
        namespace=data.namespace,
        type=NodeType.CUBE,
        current_version=0,
    )
    node_revision = create_cube_node_revision(session=session, data=data)
    save_node(session, node_revision, node, data.mode)
    return node  # type: ignore


@router.post("/nodes/{name}/columns/{column}/", status_code=201)
def link_a_dimension(
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
    if node.current.catalog.name != dimension_node.current.catalog.name:
        raise DJException(
            message=(
                "Cannot add dimension to column, because catalogs do not match: "
                f"{node.current.catalog.name}, {dimension_node.current.catalog.name}"
            ),
        )

    target_column = get_column(node.current, column)
    if dimension_column:
        # Check that the dimension column exists
        column_from_dimension = get_column(dimension_node.current, dimension_column)

        # Check the dimension column's type is compatible with the target column's type
        if not column_from_dimension.type.is_compatible(target_column.type):
            raise DJInvalidInputException(
                f"The column {target_column.name} has type {target_column.type} "
                f"and is being linked to the dimension {dimension} via the dimension"
                f" column {dimension_column}, which has type {column_from_dimension.type}."
                " These column types are incompatible and the dimension cannot be linked!",
            )

    target_column.dimension = dimension_node
    target_column.dimension_id = dimension_node.id
    target_column.dimension_column = dimension_column

    session.add(node)
    session.commit()
    session.refresh(node)
    return JSONResponse(
        status_code=201,
        content={
            "message": (
                f"Dimension node {dimension} has been successfully "
                f"linked to column {column} on node {name}"
            ),
        },
    )


@router.post("/nodes/{name}/tag/", status_code=201)
def tag_a_node(
    name: str, tag_name: str, *, session: Session = Depends(get_session)
) -> JSONResponse:
    """
    Add a tag to a node
    """
    node = get_node_by_name(session=session, name=name)
    tag = get_tag_by_name(session, name=tag_name, raise_if_not_exists=True)
    node.tags.append(tag)

    session.add(node)
    session.commit()
    session.refresh(node)
    session.refresh(tag)

    return JSONResponse(
        status_code=201,
        content={
            "message": (
                f"Node `{name}` has been successfully tagged with tag `{tag_name}`"
            ),
        },
    )


def create_new_revision_from_existing(  # pylint: disable=too-many-locals
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
                name=column_data.name,
                type=column_data.type,
                dimension_column=column_data.dimension,
            )
            for column_data in data.columns
        ]
        if data and data.columns
        else old_revision.columns,
        catalog=old_revision.catalog,
        schema_=old_revision.schema_,
        table=old_revision.table,
        parents=[],
        mode=data.mode if data and data.mode else old_revision.mode,
        materialization_configs=old_revision.materialization_configs,
    )

    # Link the new revision to its parents if the query has changed
    if (
        new_revision.type != NodeType.SOURCE
        and new_revision.query != old_revision.query
    ):
        (
            validated_node,
            dependencies_map,
            missing_parents_map,
            type_inference_failed_columns,
        ) = validate_node_data(new_revision, session)
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
        if missing_parents_map or type_inference_failed_columns:
            new_revision.status = NodeStatus.INVALID
        else:
            new_revision.status = NodeStatus.VALID
        new_revision.missing_parents = [
            MissingParent(name=missing_parent) for missing_parent in missing_parents_map
        ]
        _logger.info(
            "Parent nodes for %s (v%s): %s",
            new_revision.name,
            new_revision.version,
            [p.name for p in new_revision.parents],
        )
        new_revision.columns = validated_node.columns or []
    return new_revision


@router.patch("/nodes/{name}/", response_model=NodeOutput)
def update_a_node(
    name: str,
    data: UpdateNode,
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
def calculate_node_similarity(
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
def list_downstream_nodes(
    name: str, *, node_type: NodeType = None, session: Session = Depends(get_session)
) -> List[NodeOutput]:
    """
    List all nodes that are downstream from the given node, filterable by type.
    """
    return get_downstream_nodes(session, name, node_type)  # type: ignore


@router.get("/nodes/{name}/upstream/", response_model=List[NodeOutput])
def list_upstream_nodes(
    name: str, *, node_type: NodeType = None, session: Session = Depends(get_session)
) -> List[NodeOutput]:
    """
    List all nodes that are upstream from the given node, filterable by type.
    """
    return get_upstream_nodes(session, name, node_type)  # type: ignore
