"""
Cube related APIs.
"""

import logging
from typing import List, Optional

from fastapi import Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import get_catalog_by_name
from datajunction_server.construction.dimensions import build_dimensions_from_cube_query
from datajunction_server.database.node import Node
from datajunction_server.database.user import User
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.internal.materializations import build_cube_materialization
from datajunction_server.internal.nodes import (
    get_single_cube_revision_metadata,
    get_all_cube_revisions_metadata,
)
from datajunction_server.models import access
from datajunction_server.models.cube import (
    CubeRevisionMetadata,
    DimensionValue,
    DimensionValues,
)
from datajunction_server.models.cube_materialization import (
    DruidCubeMaterializationInput,
    UpsertCubeMaterialization,
)
from datajunction_server.models.materialization import (
    Granularity,
    MaterializationJobTypeEnum,
    MaterializationStrategy,
)
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.query import QueryCreate
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
        job=MaterializationJobTypeEnum.DRUID_CUBE,
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
        job=upsert.job.name,
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
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
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
        validate_access,
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
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
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
        validate_access,
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
        for row in result.results.__root__[0].rows
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
