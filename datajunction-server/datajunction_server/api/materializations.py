# pylint: disable=too-many-lines
"""
Node materialization related APIs.
"""
import logging
from datetime import datetime
from http import HTTPStatus
from typing import List, Optional

from fastapi import Depends
from fastapi.responses import JSONResponse
from sqlmodel import Session

from datajunction_server.api.helpers import get_node_by_name
from datajunction_server.errors import DJDoesNotExistException, DJException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.materializations import (
    create_new_materialization,
    schedule_materialization_jobs,
)
from datajunction_server.materialization.jobs import MaterializationJob
from datajunction_server.models.history import ActivityType, EntityType, History
from datajunction_server.models.materialization import (
    MaterializationConfigInfoUnified,
    MaterializationInfo,
    UpsertMaterialization,
)
from datajunction_server.models.node import NodeType
from datajunction_server.models.partition import Backfill, PartitionBackfill
from datajunction_server.models.user import User
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.typing import UTCDatetime
from datajunction_server.utils import (
    get_current_user,
    get_query_service_client,
    get_session,
    get_settings,
)

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["materializations"])


@router.post(
    "/nodes/{node_name}/materialization/",
    status_code=201,
    name="Insert or Update a Materialization for a Node",
)
def upsert_materialization(  # pylint: disable=too-many-locals
    node_name: str,
    data: UpsertMaterialization,
    *,
    session: Session = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: Optional[User] = Depends(get_current_user),
) -> JSONResponse:
    """
    Add or update a materialization of the specified node. If a node_name is specified
    for the materialization config, it will always update that named config.
    """
    node = get_node_by_name(session, node_name, with_current=True)
    if node.type == NodeType.SOURCE:
        raise DJException(
            http_status_code=HTTPStatus.BAD_REQUEST,
            message=f"Cannot set materialization config for source node `{node_name}`!",
        )
    current_revision = node.current
    old_materializations = {mat.name: mat for mat in current_revision.materializations}

    # Create a new materialization
    new_materialization = create_new_materialization(session, current_revision, data)

    # Check to see if a materialization for this engine already exists with the exact same config
    existing_materialization = old_materializations.get(new_materialization.name)
    deactivated_before = False
    if (
        existing_materialization
        and existing_materialization.config == new_materialization.config
    ):
        new_materialization.node_revision = None  # type: ignore
        # if the materialization was deactivated before, restore it
        if existing_materialization.deactivated_at is not None:
            deactivated_before = True
            existing_materialization.deactivated_at = None  # type: ignore
            session.add(
                History(
                    entity_type=EntityType.MATERIALIZATION,
                    entity_name=existing_materialization.name,
                    node=node.name,
                    activity_type=ActivityType.RESTORE,
                    details={},
                    user=current_user.username if current_user else None,
                ),
            )
            session.commit()
            session.refresh(existing_materialization)
        existing_materialization_info = query_service_client.get_materialization_info(
            node_name,
            current_revision.version,  # type: ignore
            new_materialization.name,  # type: ignore
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
        existing_materialization.config = new_materialization.config
        existing_materialization.schedule = new_materialization.schedule
        new_materialization.node_revision = None  # type: ignore
        new_materialization = existing_materialization
    else:
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

    session.add(
        History(
            entity_type=EntityType.MATERIALIZATION,
            node=node.name,
            entity_name=new_materialization.name,
            activity_type=(
                ActivityType.CREATE
                if not existing_materialization
                else ActivityType.UPDATE
            ),
            details={
                "node": node.name,
                "materialization": new_materialization.name,
            },
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()

    materialization_response = schedule_materialization_jobs(
        [new_materialization],
        query_service_client,
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
def list_node_materializations(
    node_name: str,
    show_deleted: bool = False,
    *,
    session: Session = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
) -> List[MaterializationConfigInfoUnified]:
    """
    Show all materializations configured for the node, with any associated metadata
    like urls from the materialization service, if available.
    """
    node = get_node_by_name(session, node_name, with_current=True)
    materializations = []
    for materialization in node.current.materializations:
        if not materialization.deactivated_at or show_deleted:  # pragma: no cover
            info = query_service_client.get_materialization_info(
                node_name,
                node.current.version,  # type: ignore
                materialization.name,  # type: ignore
            )
            materialization = MaterializationConfigInfoUnified(
                **materialization.dict(),
                **{"engine": materialization.engine.dict()},
                **info.dict(),
                backfills=materialization.backfills,
            )
            materializations.append(materialization)
    return materializations


@router.delete(
    "/nodes/{node_name}/materializations/",
    response_model=List[MaterializationConfigInfoUnified],
    name="Deactivate a Materialization for a Node",
)
def deactivate_node_materializations(
    node_name: str,
    materialization_name: str,
    *,
    session: Session = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: Optional[User] = Depends(get_current_user),
) -> List[MaterializationConfigInfoUnified]:
    """
    Deactivate the node materialization with the provided name.
    Also calls the query service to deactivate the associated scheduled jobs.
    """
    node = get_node_by_name(session, node_name, with_current=True)
    query_service_client.deactivate_materialization(node_name, materialization_name)
    for materialization in node.current.materializations:
        if (
            materialization.name == materialization_name
            and not materialization.deactivated_at
        ):  # pragma: no cover
            now = datetime.utcnow()
            materialization.deactivated_at = UTCDatetime(
                year=now.year,
                month=now.month,
                day=now.day,
                hour=now.hour,
                minute=now.minute,
                second=now.second,
            )
            session.add(materialization)

    session.add(
        History(
            entity_type=EntityType.MATERIALIZATION,
            entity_name=materialization_name,
            node=node.name,
            activity_type=ActivityType.DELETE,
            details={},
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()
    session.refresh(node.current)
    return JSONResponse(
        status_code=HTTPStatus.OK,
        content={
            "message": f"The materialization named `{materialization_name}` on node `{node_name}` "
            "has been successfully deactivated",
        },
    )


@router.post(
    "/nodes/{node_name}/materializations/{materialization_name}/backfill",
    status_code=201,
    name="Kick off a backfill run for a configured materialization",
)
def run_materialization_backfill(  # pylint: disable=too-many-locals
    node_name: str,
    materialization_name: str,
    backfill_spec: PartitionBackfill,
    *,
    session: Session = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: Optional[User] = Depends(get_current_user),
) -> MaterializationInfo:
    """
    Start a backfill for a configured materialization.
    """
    node = get_node_by_name(session, node_name)
    node_revision = node.current
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
    if backfill_spec.column_name not in temporal_partitions:
        raise DJDoesNotExistException(  # pragma: no cover
            f"Partition with name {backfill_spec.column_name} does not exist on node",
        )

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
        backfill_spec,
        query_service_client,
    )
    backfill = Backfill(
        materialization=materialization,
        spec=backfill_spec,
        urls=materialization_output.urls,
    )
    materialization.backfills.append(backfill)

    backfill_event = History(
        entity_type=EntityType.BACKFILL,
        node=node_name,
        activity_type=ActivityType.CREATE,
        details={
            "materialization": materialization_name,
            "partition": backfill_spec.dict(),
        },
        user=current_user.username if current_user else None,
    )
    session.add(backfill_event)
    session.commit()
    return materialization_output
