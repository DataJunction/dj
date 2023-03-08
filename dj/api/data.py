"""
Data related APIs.
"""

import logging

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlmodel import Session

from dj.api.helpers import get_node_by_name
from dj.errors import DJException
from dj.models.node import AvailabilityState, AvailabilityStateBase, NodeType
from dj.utils import get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/data/availability/{node_name}/")
def add_availability(
    node_name: str,
    data: AvailabilityStateBase,
    *,
    session: Session = Depends(get_session),
) -> JSONResponse:
    """
    Add an availability state to a node
    """
    node = get_node_by_name(session, node_name)

    # Source nodes require that any availability states set are for one of the defined tables
    node_revision = node.current
    if data.catalog != node_revision.catalog.name:
        raise DJException(
            "Cannot set availability state in different catalog: "
            f"{data.catalog}, {node_revision.catalog}",
        )
    if node.current.type == NodeType.SOURCE:
        if node_revision.schema_ != data.schema_ or node_revision.table != data.table:
            raise DJException(
                message=(
                    "Cannot set availability state, "
                    "source nodes require availability "
                    "states match the set table: "
                    f"{data.catalog}."
                    f"{data.schema_}."
                    f"{data.table} "
                    "does not match "
                    f"{node_revision.catalog.name}."
                    f"{node_revision.schema_}."
                    f"{node_revision.table} "
                ),
            )

    # Merge the new availability state with the current availability state if one exists
    if (
        node_revision.availability
        and node_revision.availability.catalog == node.current.catalog.name
        and node_revision.availability.schema_ == data.schema_
        and node_revision.availability.table == data.table
    ):
        # Currently, we do not consider type information. We should eventually check the type of
        # the partition values in order to cast them before sorting.
        data.max_partition = max(
            (
                node_revision.availability.max_partition,
                data.max_partition,
            ),
        )
        data.min_partition = min(
            (
                node_revision.availability.min_partition,
                data.min_partition,
            ),
        )

    db_new_availability = AvailabilityState.from_orm(data)
    node_revision.availability = db_new_availability
    session.add(node_revision)
    session.commit()
    return JSONResponse(
        status_code=200,
        content={"message": "Availability state successfully posted"},
    )
