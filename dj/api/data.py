"""
Data related APIs.
"""

import logging

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from dj.errors import DJException
from dj.models.node import AvailabilityState, AvailabilityStateBase, Node, NodeType
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
    new_availability = data

    try:
        statement = select(Node).where(Node.name == node_name)
        results = session.exec(statement)
        existing_node = results.one()
    except NoResultFound as exc:
        raise DJException(
            message=f"Cannot add availability state, node `{node_name}` does not exist",
        ) from exc

    # Source nodes require that any availability states set are for one of the defined tables
    if existing_node.type == NodeType.SOURCE:
        matches = False
        for table in existing_node.tables:
            if (
                table.catalog == new_availability.catalog
                and table.schema_ == new_availability.schema_
                and table.table == new_availability.table
            ):
                matches = True
        if not matches:
            raise DJException(
                message=(
                    "Cannot set availability state, "
                    "source nodes require availability "
                    "states match an existing table: "
                    f"{new_availability.catalog}."
                    f"{new_availability.schema_}."
                    f"{new_availability.table}"
                ),
            )

    # Merge the new availability state with the current availability state if one exists
    if (
        existing_node.availability
        and existing_node.availability.catalog == new_availability.catalog
        and existing_node.availability.schema_ == new_availability.schema_
        and existing_node.availability.table == new_availability.table
    ):
        # Currently, we do not consider type information. We should eventually check the type of
        # the partition values in order to cast them before sorting.
        new_availability.max_partition = max(
            (existing_node.availability.max_partition, new_availability.max_partition),
        )
        new_availability.min_partition = min(
            (existing_node.availability.min_partition, new_availability.min_partition),
        )

    db_new_availability = AvailabilityState.from_orm(new_availability)
    existing_node.availability = db_new_availability
    session.add(existing_node)
    session.commit()
    return JSONResponse(
        status_code=200,
        content={"message": "Availability state successfully posted"},
    )
