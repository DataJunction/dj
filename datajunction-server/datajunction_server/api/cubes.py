"""
Cube related APIs.
"""

import logging
from http import HTTPStatus
from typing import List, Optional

from fastapi import Depends, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import get_catalog_by_name
from datajunction_server.construction.build_v3.combiners import (
    build_combiner_sql_from_preaggs,
)
from datajunction_server.models.materialization import (
    DRUID_AGG_MAPPING,
    DRUID_SKETCH_TYPES,
)
from datajunction_server.construction.dimensions import build_dimensions_from_cube_query
from datajunction_server.database.materialization import Materialization
from datajunction_server.database.node import Node
from datajunction_server.database.user import User
from datajunction_server.errors import (
    DJInvalidInputException,
    DJQueryServiceClientException,
)
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import (
    AccessChecker,
    get_access_checker,
)
from datajunction_server.internal.materializations import build_cube_materialization
from datajunction_server.internal.nodes import (
    get_all_cube_revisions_metadata,
    get_single_cube_revision_metadata,
)
from datajunction_server.models.cube import (
    CubeRevisionMetadata,
    DimensionValue,
    DimensionValues,
)
from datajunction_server.models.cube_materialization import (
    CubeMaterializeRequest,
    CubeMaterializeResponse,
    CubeMaterializationV2Input,
    DruidCubeMaterializationInput,
    DruidCubeV3Config,
    PreAggTableInfo,
    UpsertCubeMaterialization,
)
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.preaggregation import (
    BackfillRequest,
    BackfillResponse,
    CubeBackfillInput,
)
from datajunction_server.models.materialization import (
    Granularity,
    MaterializationJobTypeEnum,
    MaterializationStrategy,
)
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.query import ColumnMetadata, QueryCreate
from datajunction_server.naming import from_amenable_name
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.utils import (
    get_current_user,
    get_query_service_client,
    get_session,
    get_settings,
)

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["cubes"])


def _build_metrics_spec(
    measure_columns,
    measure_components,
    component_aliases: dict[str, str],
) -> list[dict]:
    """
    Build Druid metricsSpec from measure columns and their components.

    Uses the merge function (Phase 2) to determine the Druid aggregator type,
    since we're loading pre-aggregated data. Falls back to longSum for unmapped types.

    Args:
        measure_columns: List of column metadata for measures
        measure_components: List of MetricComponent objects
        component_aliases: Mapping from component name to output column alias
            e.g., {"account_id_hll_e7b21ce4": "approx_unique_accounts_rating"}
    """
    # Build lookup from component name to component
    component_by_name = {comp.name: comp for comp in measure_components}

    # Build reverse lookup from alias to component
    # component_aliases: {internal_name -> alias}
    # We need: {alias -> internal_name} to look up by column name
    alias_to_component_name = {alias: name for name, alias in component_aliases.items()}

    metrics = []
    for col in measure_columns:
        # Try direct lookup first, then alias lookup
        component = component_by_name.get(col.name)
        if not component:
            # Look up by alias
            internal_name = alias_to_component_name.get(col.name)
            if internal_name:
                component = component_by_name.get(internal_name)

        druid_type = "longSum"  # Default fallback

        if component:
            # Use merge function for pre-aggregated data, fall back to aggregation
            agg_func = component.merge or component.aggregation
            if agg_func:
                key = (col.type, agg_func.lower())
                if key in DRUID_AGG_MAPPING:
                    druid_type = DRUID_AGG_MAPPING[key]

        metric_spec = {
            "fieldName": col.name,
            "name": col.name,
            "type": druid_type,
        }

        # HLL sketches need additional configuration
        if druid_type in DRUID_SKETCH_TYPES:
            metric_spec["lgK"] = 12  # Log2 of K, controls precision (4-21)
            metric_spec["tgtHllType"] = "HLL_4"  # HLL_4, HLL_6, or HLL_8

        metrics.append(metric_spec)

    return metrics


@router.get("/cubes", name="Get all Cubes")
async def get_all_cubes(
    *,
    session: AsyncSession = Depends(get_session),
    catalog: Optional[str] = Query(
        None,
        description="Filter to include only cubes available in a specific catalog",
    ),
    page: int = Query(1, ge=1, description="Page number (starting from 1)"),
    page_size: int = Query(
        10,
        ge=1,
        le=1000,
        description="Number of items per page (max 1000)",
    ),
) -> list[CubeRevisionMetadata]:
    """
    Get information on all cubes
    """
    return await get_all_cube_revisions_metadata(
        session=session,
        catalog=catalog,
        page=page,
        page_size=page_size,
    )


@router.get("/cubes/{name}/", name="Get a Cube")
async def get_cube(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> CubeRevisionMetadata:
    """
    Get information on a cube
    """
    return await get_single_cube_revision_metadata(session, name)


@router.get("/cubes/{name}/versions/{version}", name="Get a Cube Revision")
async def get_cube_by_version(
    name: str,
    version: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> CubeRevisionMetadata:
    """
    Get information on a specific cube revision
    """
    return await get_single_cube_revision_metadata(session, name, version=version)


@router.get("/cubes/{name}/materialization", name="Cube Materialization Config")
async def cube_materialization_info(
    name: str,
    session: AsyncSession = Depends(get_session),
) -> DruidCubeMaterializationInput:
    """
    The standard cube materialization config. DJ makes sensible materialization choices
    where possible.

    Requirements:
    - The cube must have a temporal partition column specified.
    - The job strategy will always be "incremental time".

    Outputs:
    "measures_materializations":
        We group the metrics by parent node. Then we try to pre-aggregate each parent node as
        much as possible to prepare for metric queries on the cube's dimensions.
    "combiners":
        We combine each set of measures materializations on their shared grain. Note that we don't
        support materializing cubes with measures materializations that don't share the same grain.
        However, we keep `combiners` as a list in the eventual future where we support that.
    "metrics":
        We include a list of metrics, their required measures, and the derived expression (e.g., the
        expression used by the metric that makes use of the pre-aggregated measures)

    Once we create a scheduled materialization workflow, we freeze the metadata for that particular
    materialized dataset. This allows us to reconstruct metrics SQL from the dataset when needed.
    To request metrics from the materialized cube, use the metrics' measures metadata.
    """
    node = await Node.get_cube_by_name(session, name)
    temporal_partitions = node.current.temporal_partition_columns()  # type: ignore
    if len(temporal_partitions) != 1:
        raise DJInvalidInputException(
            "The cube must have a single temporal partition column set "
            "in order for it to be materialized.",
        )
    temporal_partition = temporal_partitions[0] if temporal_partitions else None
    granularity_lookback_defaults = {
        Granularity.MINUTE: "1 MINUTE",
        Granularity.HOUR: "1 HOUR",
        Granularity.DAY: "1 DAY",
        Granularity.WEEK: "1 WEEK",
        Granularity.MONTH: "1 MONTH",
        Granularity.QUARTER: "1 QUARTER",
        Granularity.YEAR: "1 YEAR",
    }
    granularity_cron_defaults = {
        Granularity.MINUTE: "* * * * *",  # Runs every minute
        Granularity.HOUR: "0 * * * *",  # Runs at the start of every hour
        Granularity.DAY: "0 0 * * *",  # Runs at midnight every day
        Granularity.WEEK: "0 0 * * 0",  # Runs at midnight on Sundays
        Granularity.MONTH: "0 0 1 * *",  # Runs at midnight on the first of every month
        Granularity.QUARTER: "0 0 1 */3 *",  # Runs at midnight on the first day of each quarter
        Granularity.YEAR: "0 0 1 1 *",  # Runs at midnight on January 1st every year
    }
    upsert = UpsertCubeMaterialization(
        job=MaterializationJobTypeEnum.DRUID_CUBE.value.name,
        strategy=(
            MaterializationStrategy.INCREMENTAL_TIME
            if temporal_partition
            else MaterializationStrategy.FULL
        ),
        lookback_window=granularity_lookback_defaults.get(
            temporal_partition.partition.granularity,
            granularity_lookback_defaults[Granularity.DAY],
        ),
        schedule=granularity_cron_defaults.get(
            temporal_partition.partition.granularity,
            granularity_cron_defaults[Granularity.DAY],
        ),
    )
    cube_config = await build_cube_materialization(
        session,
        node.current,  # type: ignore
        upsert,
    )
    return DruidCubeMaterializationInput(
        name="",
        cube=cube_config.cube,
        dimensions=cube_config.dimensions,
        metrics=cube_config.metrics,
        strategy=upsert.strategy,
        schedule=upsert.schedule,
        job=upsert.job.name,  # type: ignore
        measures_materializations=cube_config.measures_materializations,
        combiners=cube_config.combiners,
    )


@router.get("/cubes/{name}/dimensions/sql", name="Dimensions SQL for Cube")
async def get_cube_dimension_sql(
    name: str,
    *,
    dimensions: List[str] = Query([], description="Dimensions to get values for"),
    filters: Optional[str] = Query(
        None,
        description="Filters on dimensional attributes",
    ),
    limit: Optional[int] = Query(
        None,
        description="Number of rows to limit the data retrieved to",
    ),
    include_counts: bool = False,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> TranslatedSQL:
    """
    Generates SQL to retrieve all unique values of a dimension for the cube
    """
    node = await Node.get_cube_by_name(session, name)
    node_revision = node.current  # type: ignore
    return await build_dimensions_from_cube_query(
        session,
        node_revision,
        dimensions,
        current_user,
        access_checker,
        filters,
        limit,
        include_counts,
    )


@router.get(
    "/cubes/{name}/dimensions/data",
    name="Dimensions Values for Cube",
)
async def get_cube_dimension_values(
    name: str,
    *,
    dimensions: List[str] = Query([], description="Dimensions to get values for"),
    filters: Optional[str] = Query(
        None,
        description="Filters on dimensional attributes",
    ),
    limit: Optional[int] = Query(
        None,
        description="Number of rows to limit the data retrieved to",
    ),
    include_counts: bool = False,
    async_: bool = False,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: User = Depends(get_current_user),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> DimensionValues:
    """
    All unique values of a dimension from the cube
    """
    request_headers = dict(request.headers)
    node = await Node.get_cube_by_name(session, name)
    cube = node.current  # type: ignore
    translated_sql = await build_dimensions_from_cube_query(
        session,
        cube,
        dimensions,
        current_user,
        access_checker,
        filters,
        limit,
        include_counts,
    )
    if cube.availability:
        catalog = await get_catalog_by_name(  # pragma: no cover
            session,
            cube.availability.catalog,  # type: ignore
        )
    else:
        catalog = cube.catalog
    query_create = QueryCreate(
        engine_name=catalog.engines[0].name,
        catalog_name=catalog.name,
        engine_version=catalog.engines[0].version,
        submitted_query=translated_sql.sql,
        async_=async_,
    )
    result = query_service_client.submit_query(
        query_create,
        request_headers=request_headers,
    )
    count_column = [
        idx
        for idx, col in enumerate(translated_sql.columns)  # type: ignore
        if col.name == "count"
    ]
    dimension_values = [  # pragma: no cover
        DimensionValue(
            value=row[0 : count_column[0]] if count_column else row,
            count=row[count_column[0]] if count_column else None,
        )
        for row in result.results.root[0].rows
    ]
    return DimensionValues(  # pragma: no cover
        dimensions=[
            from_amenable_name(col.name)
            for col in translated_sql.columns  # type: ignore
            if col.name != "count"
        ],
        values=dimension_values,
        cardinality=len(dimension_values),
    )


@router.post(
    "/cubes/{name}/materialize",
    response_model=CubeMaterializeResponse,
    name="Materialize Cube to Druid",
)
async def materialize_cube(
    name: str,
    data: CubeMaterializeRequest,
    request: Request,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
) -> CubeMaterializeResponse:
    """
    Create a Druid cube materialization workflow.

    This endpoint generates all the information needed for a Druid cube workflow:

    1. **Pre-agg table dependencies**: The Druid workflow should wait (via VTTS)
       for these tables to be available before starting ingestion.

    2. **Combined SQL**: SQL that reads from the pre-agg tables with re-aggregation,
       combining multiple grain groups via FULL OUTER JOIN.

    3. **Druid spec**: Ingestion specification for Druid.

    The typical flow is:
    - Pre-agg workflows write to: `{preagg_catalog}.{preagg_schema}.{node}_preagg_{hash}`
    - Druid workflow waits on those tables' VTTS
    - Once available, Druid workflow runs the combined SQL and ingests to Druid

    Args:
        name: Cube name
        data: Materialization configuration (schedule, strategy, etc.)

    Returns:
        CubeMaterializeResponse with pre-agg dependencies, combined SQL, and Druid spec.
    """
    # Get the cube
    node = await Node.get_cube_by_name(session, name)
    if not node:
        raise DJInvalidInputException(
            message=f"Cube '{name}' not found",
            http_status_code=HTTPStatus.NOT_FOUND,
        )
    cube_revision = node.current

    if not cube_revision:
        raise DJInvalidInputException(  # pragma: no cover
            message=f"Cube '{name}' has no current revision",
            http_status_code=HTTPStatus.NOT_FOUND,
        )

    # Build combined SQL from pre-agg tables
    try:
        (
            combined_result,
            preagg_table_refs,
            temporal_partition_info,
        ) = await build_combiner_sql_from_preaggs(
            session=session,
            metrics=cube_revision.cube_node_metrics,
            dimensions=cube_revision.cube_node_dimensions,
            filters=None,
            dialect=Dialect.SPARK,
        )
    except Exception as e:  # pragma: no cover
        raise DJInvalidInputException(  # pragma: no cover
            message=f"Failed to generate combined SQL: {e!s}",
            http_status_code=HTTPStatus.BAD_REQUEST,
        ) from e

    # For incremental strategy, we need a temporal partition
    if (
        data.strategy == MaterializationStrategy.INCREMENTAL_TIME
        and not temporal_partition_info
    ):
        raise DJInvalidInputException(
            message=(
                "Could not auto-detect temporal partition from pre-aggregations. "
                "Please ensure the source nodes have temporal partitions configured, "
                "or use FULL materialization strategy."
            ),
            http_status_code=HTTPStatus.BAD_REQUEST,
        )

    # Build pre-agg table info
    preagg_tables = []
    for i, table_ref in enumerate(preagg_table_refs):
        # Extract parent node from the grain groups used in combiner
        parent_name = combined_result.columns[0].semantic_name or "unknown"
        # Get grain from the combiner result
        grain = combined_result.shared_dimensions

        preagg_tables.append(
            PreAggTableInfo(
                table_ref=table_ref,
                parent_node=parent_name,
                grain=grain,
            ),
        )

    # Generate Druid datasource name (versioned to prevent overwrites)
    safe_name = name.replace(".", "_")
    safe_version = str(cube_revision.version).replace(".", "_")
    druid_datasource = data.druid_datasource or f"dj_{safe_name}_{safe_version}"

    # Build Druid spec
    dimension_columns = [
        col.name for col in combined_result.columns if col.semantic_type == "dimension"
    ]
    measure_columns = [
        col
        for col in combined_result.columns
        if col.semantic_type in ("metric", "metric_component", "measure")
    ]

    # Build timestamp spec from auto-detected temporal partition
    timestamp_column = (
        temporal_partition_info.column_name if temporal_partition_info else None
    )
    timestamp_format = (
        temporal_partition_info.format if temporal_partition_info else "auto"
    )
    segment_granularity = (
        temporal_partition_info.granularity.upper()
        if temporal_partition_info and temporal_partition_info.granularity
        else "DAY"
    )

    # Validate that the timestamp column is in the output columns
    all_output_col_names = {col.name for col in combined_result.columns}
    if timestamp_column and timestamp_column not in all_output_col_names:
        raise DJInvalidInputException(  # pragma: no cover
            message=(
                f"Detected temporal partition column '{timestamp_column}' is not in the "
                f"combined query output columns ({all_output_col_names}). "
                f"Please ensure you've selected a date/time dimension "
                f"(e.g., the date column) in your cube dimensions."
            ),
            http_status_code=HTTPStatus.BAD_REQUEST,
        )

    druid_spec = {
        "dataSchema": {
            "dataSource": druid_datasource,
            "parser": {
                "parseSpec": {
                    "format": "parquet",
                    "dimensionsSpec": {
                        "dimensions": sorted(dimension_columns),
                    },
                    "timestampSpec": {
                        "column": timestamp_column,
                        "format": timestamp_format or "auto",
                    },
                },
            },
            "metricsSpec": _build_metrics_spec(
                measure_columns,
                combined_result.measure_components,
                combined_result.component_aliases,
            ),
            "granularitySpec": {
                "type": "uniform",
                "segmentGranularity": segment_granularity,
                "intervals": [],  # Set at runtime
            },
        },
        "tuningConfig": {
            "partitionsSpec": {
                "targetPartitionSize": 5000000,
                "type": "hashed",
            },
            "useCombiner": True,
            "type": "hadoop",
        },
    }

    # Convert columns to ColumnMetadata
    output_columns = [
        ColumnMetadata(
            name=col.name,
            type=col.type,
            semantic_entity=col.semantic_name,
            semantic_type=col.semantic_type,
        )
        for col in combined_result.columns
    ]

    # Build the v2 input for the query service
    v2_input = CubeMaterializationV2Input(
        cube_name=node.name,
        cube_version=str(cube_revision.version),
        preagg_tables=preagg_tables,
        combined_sql=combined_result.sql,
        combined_columns=output_columns,
        combined_grain=combined_result.shared_dimensions,
        druid_datasource=druid_datasource,
        druid_spec=druid_spec,
        timestamp_column=timestamp_column,
        timestamp_format=timestamp_format or "yyyyMMdd",
        strategy=data.strategy,
        schedule=data.schedule,
        lookback_window=data.lookback_window,
    )

    # Call the query service to create the workflow
    request_headers = dict(request.headers)
    try:
        mat_result = query_service_client.materialize_cube_v2(
            v2_input,
            request_headers=request_headers,
        )
        workflow_urls = mat_result.urls
        message = (
            f"Cube materialization workflow created. "
            f"Workflow waits on {len(preagg_tables)} pre-agg table(s) before Druid ingestion. "
            f"Workflow URLs: {workflow_urls}"
        )
    except Exception as e:
        _logger.warning(
            "Failed to create workflow for cube=%s: %s. Returning response without workflow.",
            name,
            str(e),
        )
        workflow_urls = []
        message = (
            f"Cube materialization prepared (workflow creation failed: {e}). "
            f"Druid workflow should wait on {len(preagg_tables)} pre-agg table(s) before ingestion."
        )

    # Persist materialization record on the cube's node revision
    materialization_name = "druid_cube_v3"
    existing_mats = await Materialization.get_by_names(
        session,
        cube_revision.id,
        [materialization_name],
    )

    # Build metrics list with combiner expressions
    metrics_list = []
    for metric_name in cube_revision.cube_node_metrics:
        metric_expression = combined_result.metric_combiners.get(metric_name)
        short_name = metric_name.split(".")[-1]
        metrics_list.append(
            {
                "node": metric_name,
                "name": short_name,
                "metric_expression": metric_expression,
                "metric": {
                    "name": metric_name,
                    "display_name": short_name.replace("_", " ").title(),
                },
            },
        )

    # Build V3 config using the proper Pydantic model
    mat_config = DruidCubeV3Config(
        druid_datasource=druid_datasource,
        preagg_tables=preagg_tables,
        combined_sql=combined_result.sql,
        combined_columns=output_columns,
        combined_grain=combined_result.shared_dimensions,
        measure_components=combined_result.measure_components,
        component_aliases=combined_result.component_aliases,
        cube_metrics=cube_revision.cube_node_metrics,
        metrics=metrics_list,
        timestamp_column=timestamp_column,
        timestamp_format=timestamp_format or "yyyyMMdd",
        workflow_urls=workflow_urls,
    )

    if existing_mats:
        # Update existing materialization
        mat = existing_mats[0]
        mat.strategy = data.strategy
        mat.schedule = data.schedule
        mat.config = mat_config.model_dump()
        _logger.info("Updated materialization record for cube=%s", name)
    else:
        # Create new materialization
        mat = Materialization(
            node_revision_id=cube_revision.id,
            name=materialization_name,
            strategy=data.strategy,
            schedule=data.schedule,
            config=mat_config.model_dump(),
            job="DruidCubeMaterializationJob",
        )
        session.add(mat)
        _logger.info("Created materialization record for cube=%s", name)

    await session.commit()

    return CubeMaterializeResponse(
        cube=NodeNameVersion(
            name=node.name,
            version=str(cube_revision.version),
        ),
        druid_datasource=druid_datasource,
        preagg_tables=preagg_tables,
        combined_sql=combined_result.sql,
        combined_columns=output_columns,
        combined_grain=combined_result.shared_dimensions,
        druid_spec=druid_spec,
        strategy=data.strategy,
        schedule=data.schedule,
        lookback_window=data.lookback_window,
        metric_combiners=combined_result.metric_combiners,
        workflow_urls=workflow_urls,
        message=message,
    )


@router.delete(
    "/cubes/{name}/materialize",
    name="Deactivate Cube Materialization",
)
async def deactivate_cube_materialization(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    request: Request,
) -> JSONResponse:
    """
    Deactivate (remove) the Druid cube materialization for this cube.

    This will:
    1. Remove the materialization record from the cube
    2. Optionally deactivate the workflow in the query service (if supported)
    """
    node = await Node.get_cube_by_name(session, name)
    if not node:
        raise DJInvalidInputException(
            message=f"Cube {name} not found",
            http_status_code=HTTPStatus.NOT_FOUND,
        )

    cube_revision = node.current

    # Find and delete the druid_cube_v3 materialization
    materialization_name = "druid_cube_v3"
    existing_mats = await Materialization.get_by_names(
        session,
        cube_revision.id,
        [materialization_name],
    )

    if not existing_mats:
        raise DJInvalidInputException(
            message=f"No Druid cube materialization found for cube {name}",
            http_status_code=HTTPStatus.NOT_FOUND,
        )

    mat = existing_mats[0]

    # Try to deactivate the workflow in the query service
    request_headers = dict(request.headers)
    try:
        query_service_client.deactivate_cube_workflow(
            name,
            version=cube_revision.version,
            request_headers=request_headers,
        )
        _logger.info(
            "Deactivated workflow for cube=%s version=%s",
            name,
            cube_revision.version,
        )
    except Exception as e:
        _logger.warning(
            "Failed to deactivate workflow for cube=%s: %s (continuing with deletion)",
            name,
            str(e),
        )

    # Delete the materialization record
    await session.delete(mat)
    await session.commit()

    _logger.info("Deleted materialization record for cube=%s", name)

    return JSONResponse(
        status_code=HTTPStatus.OK,
        content={
            "message": f"Cube materialization deactivated for {name}",
        },
    )


@router.post(
    "/cubes/{name}/backfill",
    response_model=BackfillResponse,
    name="Run Cube Backfill",
)
async def run_cube_backfill(
    name: str,
    data: BackfillRequest,
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
) -> BackfillResponse:
    """
    Run a backfill for the cube over the specified date range.

    This triggers the cube's backfill workflow with the given start_date
    and end_date. The workflow iterates through each date partition
    and re-runs the cube materialization for that date.

    Prerequisites:
    - Cube materialization must be scheduled (via POST /cubes/{name}/materialize)
    """
    from datetime import date as date_type

    # Verify the cube exists
    node = await Node.get_cube_by_name(session, name)
    if not node:
        raise DJInvalidInputException(
            message=f"Cube '{name}' not found",
            http_status_code=HTTPStatus.NOT_FOUND,
        )

    # Verify cube has a materialization
    cube_revision = node.current
    materialization_name = "druid_cube_v3"
    existing_mats = await Materialization.get_by_names(
        session,
        cube_revision.id,
        [materialization_name],
    )

    if not existing_mats:
        raise DJInvalidInputException(
            message=(
                f"Cube '{name}' has no materialization. "
                "Use POST /cubes/{name}/materialize first."
            ),
            http_status_code=HTTPStatus.BAD_REQUEST,
        )

    # Default end_date to today
    end_date = data.end_date or date_type.today()

    # Build backfill input for query service
    backfill_input = CubeBackfillInput(
        cube_name=name,
        cube_version=str(cube_revision.version),
        start_date=data.start_date,
        end_date=end_date,
    )

    # Call query service
    _logger.info(
        "Running backfill for cube=%s from %s to %s",
        name,
        data.start_date,
        end_date,
    )
    request_headers = dict(request.headers)

    try:
        backfill_result = query_service_client.run_cube_backfill(
            backfill_input,
            request_headers=request_headers,
        )
    except Exception as e:
        _logger.exception(
            "Failed to run backfill for cube=%s: %s",
            name,
            str(e),
        )
        raise DJQueryServiceClientException(
            message=f"Failed to run cube backfill: {e}",
        )

    job_url = backfill_result.get("job_url", "")
    _logger.info(
        "Started backfill for cube=%s job_url=%s",
        name,
        job_url,
    )

    return BackfillResponse(
        job_url=job_url,
        start_date=data.start_date,
        end_date=end_date,
        status="running",
    )
