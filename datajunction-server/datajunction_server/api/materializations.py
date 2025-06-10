"""
Node materialization related APIs.
"""

import logging
from datetime import datetime
from http import HTTPStatus
from typing import Callable, List

from fastapi import Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.api.helpers import get_save_history, get_materialization_info
from datajunction_server.database import Node, NodeRevision
from datajunction_server.database.backfill import Backfill
from datajunction_server.database.column import Column, ColumnAttribute
from datajunction_server.database.history import History
from datajunction_server.database.user import User
from datajunction_server.errors import DJDoesNotExistException, DJInvalidInputException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.internal.materializations import (
    create_new_materialization,
    schedule_materialization_jobs,
)
from datajunction_server.materialization.jobs import MaterializationJob
from datajunction_server.models import access
from datajunction_server.models.base import labelize
from datajunction_server.models.cube_materialization import UpsertCubeMaterialization
from datajunction_server.models.materialization import (
    MaterializationConfigInfoUnified,
    MaterializationInfo,
    MaterializationJobTypeEnum,
    MaterializationStrategy,
    UpsertMaterialization,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import PartitionBackfill
from datajunction_server.naming import amenable_name
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.typing import UTCDatetime
from datajunction_server.utils import (
    get_and_update_current_user,
    get_query_service_client,
    get_session,
    get_settings,
)

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["materializations"])


@router.get(
    "/materialization/info",
    status_code=200,
    name="Materialization Jobs Info",
)
def materialization_jobs_info() -> JSONResponse:
    """
    Materialization job types and strategies
    """
    return JSONResponse(
        status_code=200,
        content={
            "job_types": [value.value.dict() for value in MaterializationJobTypeEnum],
            "strategies": [
                {"name": value, "label": labelize(value)}
                for value in MaterializationStrategy
            ],
        },
    )


@router.post(
    "/nodes/{node_name}/materialization/",
    status_code=201,
    name="Insert or Update a Materialization for a Node",
)
async def upsert_materialization(
    node_name: str,
    data: UpsertMaterialization | UpsertCubeMaterialization,
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
) -> JSONResponse:
    """
    Add or update a materialization of the specified node. If a node_name is specified
    for the materialization config, it will always update that named config.
    """
    request_headers = dict(request.headers)
    node = await Node.get_by_name(session, node_name, raise_if_not_exists=True)
    if node.type == NodeType.SOURCE:  # type: ignore
        raise DJInvalidInputException(
            http_status_code=HTTPStatus.BAD_REQUEST,
            message=f"Cannot set materialization config for source node `{node_name}`!",
        )
    if node.type == NodeType.CUBE:  # type: ignore
        node = await Node.get_cube_by_name(session, node_name)
    _logger.info(
        "Upserting materialization for node=%s version=%s",
        node.name,  # type: ignore
        node.current_version,  # type: ignore
    )

    current_revision = node.current  # type: ignore
    old_materializations = {mat.name: mat for mat in current_revision.materializations}

    if data.strategy == MaterializationStrategy.INCREMENTAL_TIME:
        if not node.current.temporal_partition_columns():  # type: ignore
            raise DJInvalidInputException(
                http_status_code=HTTPStatus.BAD_REQUEST,
                message="Cannot create materialization with strategy "
                f"`{data.strategy}` without specifying a time partition column!",
            )

    # Create a new materialization
    new_materialization = await create_new_materialization(
        session,
        current_revision,
        data,
        validate_access,
        current_user=current_user,
    )

    # Check to see if a materialization for this engine already exists with the exact same config
    existing_materialization = old_materializations.get(new_materialization.name)
    deactivated_before = False
    if (
        existing_materialization
        and existing_materialization.config == new_materialization.config
    ):
        _logger.info(
            "Existing materialization found for node=%s version=%s",
            node.name,  # type: ignore
            node.current_version,  # type: ignore
        )
        new_materialization.node_revision = None  # type: ignore
        # if the materialization was deactivated before, restore it
        if existing_materialization.deactivated_at is not None:
            deactivated_before = True
            existing_materialization.deactivated_at = None  # type: ignore
            await save_history(
                event=History(
                    entity_type=EntityType.MATERIALIZATION,
                    entity_name=existing_materialization.name,
                    node=node.name,  # type: ignore
                    activity_type=ActivityType.RESTORE,
                    details={},
                    user=current_user.username,
                ),
                session=session,
            )
            await session.commit()
            await session.refresh(existing_materialization)
        existing_materialization_info = query_service_client.get_materialization_info(
            node_name,
            current_revision.version,  # type: ignore
            current_revision.type,
            new_materialization.name,  # type: ignore
            request_headers=request_headers,
        )
        _logger.info(
            "Refresh materialization workflows for node=%s version=%s",
            node.name,  # type: ignore
            node.current_version,  # type: ignore
        )
        await schedule_materialization_jobs(
            session,
            node_revision_id=current_revision.id,
            materialization_names=[new_materialization.name],
            query_service_client=get_query_service_client(request),  # type: ignore
            request_headers=request_headers,
        )
        return JSONResponse(
            status_code=HTTPStatus.CREATED,
            content={
                "message": (
                    f"The same materialization config with name `{new_materialization.name}` "
                    f"already exists for node `{node_name}` so no update was performed."
                    if not deactivated_before
                    else f"The same materialization config with name `{new_materialization.name}` "
                    f"already exists for node `{node_name}` but was deactivated. It has now been "
                    f"restored."
                ),
                "info": existing_materialization_info.dict(),
            },
        )
    # If changes are detected, update the existing or save the new materialization
    if existing_materialization:
        _logger.info(
            "Updating existing materialization for node=%s version=%s",
            node.name,  # type: ignore
            node.current_version,  # type: ignore
        )
        existing_materialization.config = new_materialization.config
        existing_materialization.schedule = new_materialization.schedule
        new_materialization.node_revision = None  # type: ignore
        new_materialization = existing_materialization
        new_materialization.deactivated_at = None
    else:
        _logger.info(
            "Adding new materialization for node=%s version=%s",
            node.name,  # type: ignore
            node.current_version,  # type: ignore
        )
        unchanged_existing_materializations = [
            config
            for config in current_revision.materializations
            if config.name != new_materialization.name
        ]
        current_revision.materializations = unchanged_existing_materializations + [  # type: ignore
            new_materialization,
        ]

    # This will add the materialization config, the new node rev, and update the node's version.
    session.add(current_revision)
    session.add(node)

    await save_history(
        event=History(
            entity_type=EntityType.MATERIALIZATION,
            node=node.name,  # type: ignore
            entity_name=new_materialization.name,
            activity_type=(
                ActivityType.CREATE
                if not existing_materialization
                else ActivityType.UPDATE
            ),
            details={
                "node": node.name,  # type: ignore
                "materialization": new_materialization.name,
            },
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()
    _logger.info(
        "Scheduling materialization workflows for node=%s version=%s",
        node.name,  # type: ignore
        node.current_version,  # type: ignore
    )
    materialization_response = await schedule_materialization_jobs(
        session,
        node_revision_id=current_revision.id,
        materialization_names=[new_materialization.name],
        query_service_client=get_query_service_client(request),  # type: ignore
        request_headers=request_headers,
    )
    return JSONResponse(
        status_code=200,
        content={
            "message": (
                f"Successfully updated materialization config named `{new_materialization.name}` "
                f"for node `{node_name}`"
            ),
            "urls": [output.urls for output in materialization_response.values()],
        },
    )


@router.get(
    "/nodes/{node_name}/materializations/",
    response_model=List[MaterializationConfigInfoUnified],
    name="List Materializations for a Node",
)
async def list_node_materializations(
    node_name: str,
    show_inactive: bool = False,
    include_all_revisions: bool = False,
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
) -> List[MaterializationConfigInfoUnified]:
    """
    Show all materializations configured for the node, with any associated metadata
    like urls from the materialization service, if available.

    show_inactive: bool - Show materializations that have a deactivated_at timestamp set
    include_all_revisions: bool - Show  materializations for all revisions of the node
    """
    request_headers = dict(request.headers)

    # If materializations from all revisions are requested,
    # this includes the joined load to pull all node revisions
    node = await Node.get_by_name(
        session,
        node_name,
        options=[
            joinedload(Node.revisions).options(*NodeRevision.default_load_options()),
        ]
        if include_all_revisions
        else [],
        raise_if_not_exists=True,
    )

    return get_materialization_info(
        query_service_client=query_service_client,
        node=node,  # type: ignore
        include_all_revisions=include_all_revisions,
        show_inactive=show_inactive,
        request_headers=request_headers,
    )


@router.delete(
    "/nodes/{node_name}/materializations/",
    response_model=None,
    name="Deactivate a Materialization for a Node",
)
async def deactivate_node_materializations(
    node_name: str,
    materialization_name: str,
    node_version: str | None = None,
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
) -> JSONResponse:
    """
    Deactivate the node materialization with the provided name.
    Also calls the query service to deactivate the associated scheduled jobs.

    If node_version not provided, it will deactivate the materialization
    for the current version of the node.
    """
    request_headers = dict(request.headers)

    # find the node revision to deactivate the materialization for
    node_revision = None
    if node_version:
        stmt = (
            select(NodeRevision)
            .options(*NodeRevision.default_load_options())
            .where(NodeRevision.name == node_name, NodeRevision.version == node_version)
        )
        result = await session.execute(stmt)
        node_revision = result.scalars().first()
        if not node_revision:
            raise DJDoesNotExistException(  # pragma: no cover
                f"Node revision with version '{node_version}' not found for node {node_name} .",
            )
    else:
        node = await Node.get_by_name(session, node_name)
        node_revision = node.current  # type: ignore

    # find the materialization to deactivate
    materialization_to_deactivate = None
    for materialization in node_revision.materializations:  # type: ignore
        if materialization.name == materialization_name:
            materialization_to_deactivate = materialization
            break
    if not materialization_to_deactivate:
        raise DJDoesNotExistException(
            f"Materialization with name '{materialization_name}' not found on "
            f"version {node_version} of node {node_name} .",
        )
    elif materialization_to_deactivate.deactivated_at:  # pragma: no cover
        return JSONResponse(
            status_code=HTTPStatus.OK,
            content={
                "message": f"Materialization named `{materialization_name}` on node `{node_name}` "
                f"version `{node_revision.version}` has been already inactive.",  # type: ignore
            },
        )
    # do the deactivation
    query_service_client.deactivate_materialization(
        node_name,
        materialization_name,
        node_version=node_revision.version,  # type: ignore
        request_headers=request_headers,
    )
    now = datetime.utcnow()
    materialization_to_deactivate.deactivated_at = UTCDatetime(
        year=now.year,
        month=now.month,
        day=now.day,
        hour=now.hour,
        minute=now.minute,
        second=now.second,
    )
    session.add(materialization_to_deactivate)
    # save the history event
    await save_history(
        event=History(
            entity_type=EntityType.MATERIALIZATION,
            entity_name=materialization_name,
            node=node_name,
            version=node_revision.version,  # type: ignore
            activity_type=ActivityType.DELETE,
            details={},
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()
    # await session.refresh(node.current)  # type: ignore
    return JSONResponse(
        status_code=HTTPStatus.OK,
        content={
            "message": f"Materialization named `{materialization_name}` on node `{node_name}` "
            f"version `{node_revision.version}` has been successfully deactivated",
        },
    )


@router.post(
    "/nodes/{node_name}/materializations/{materialization_name}/backfill",
    status_code=201,
    name="Kick off a backfill run for a configured materialization",
)
async def run_materialization_backfill(
    node_name: str,
    materialization_name: str,
    backfill_partitions: List[PartitionBackfill],
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
) -> MaterializationInfo:
    """
    Start a backfill for a configured materialization.
    """
    request_headers = dict(request.headers)
    node = await Node.get_by_name(
        session,
        node_name,
        options=[
            joinedload(Node.current).options(
                selectinload(NodeRevision.columns).options(
                    selectinload(Column.attributes).joinedload(
                        ColumnAttribute.attribute_type,
                    ),
                    selectinload(Column.dimension),
                    selectinload(Column.partition),
                ),
                selectinload(NodeRevision.materializations),
            ),
        ],
    )
    node_revision = node.current  # type: ignore
    materializations = [
        mat
        for mat in node_revision.materializations
        if mat.name == materialization_name
    ]
    if not materializations:
        raise DJDoesNotExistException(
            f"Materialization with name {materialization_name} not found",
        )

    materialization = materializations[0]
    temporal_partitions = {
        col.name: col for col in node_revision.temporal_partition_columns()
    }
    categorical_partitions = {
        col.name: col for col in node_revision.categorical_partition_columns()
    }
    for backfill_spec in backfill_partitions:
        if backfill_spec.column_name not in set(temporal_partitions).union(
            set(categorical_partitions),
        ):
            raise DJDoesNotExistException(  # pragma: no cover
                f"Partition with name {backfill_spec.column_name} does not exist on node",
            )
        backfill_spec.column_name = amenable_name(backfill_spec.column_name)
    materialization_jobs = {
        cls.__name__: cls for cls in MaterializationJob.__subclasses__()
    }
    clazz = materialization_jobs.get(materialization.job)
    if not clazz:
        raise DJDoesNotExistException(  # pragma: no cover
            f"Materialization job {materialization.job} does not exist",
        )

    materialization_output = clazz().run_backfill(  # type: ignore
        materialization,
        backfill_partitions,
        query_service_client,
        request_headers=request_headers,
    )
    backfill = Backfill(
        materialization=materialization,
        spec=[backfill_partition.dict() for backfill_partition in backfill_partitions],
        urls=materialization_output.urls,
    )
    materialization.backfills.append(backfill)

    await save_history(
        event=History(
            entity_type=EntityType.BACKFILL,
            node=node_name,
            activity_type=ActivityType.CREATE,
            details={
                "materialization": materialization_name,
                "partition": [
                    backfill_partition.dict()
                    for backfill_partition in backfill_partitions
                ],
            },
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()
    return materialization_output
