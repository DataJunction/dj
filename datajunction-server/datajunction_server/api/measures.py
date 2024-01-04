"""
Measures related APIs.
"""

import logging
from typing import List, Optional

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from datajunction_server.api.helpers import get_node_by_name
from datajunction_server.database.column import Column
from datajunction_server.database.measure import Measure
from datajunction_server.errors import DJAlreadyExistsException, DJDoesNotExistException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.measure import (
    CreateMeasure,
    EditMeasure,
    MeasureOutput,
    NodeColumn,
)
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["measures"])


def get_measure_by_name(
    session: Session,
    measure_name: str,
    raise_if_not_exists: bool = True,
) -> Measure:
    """Retrieve a measure by name"""
    measure = (
        session.execute(select(Measure).where(Measure.name == measure_name))
        .unique()
        .scalars()
        .one_or_none()
    )
    if raise_if_not_exists and not measure:
        raise DJDoesNotExistException(
            message=f"Measure with name `{measure_name}` does not exist",
        )
    return measure


def get_node_columns(session: Session, node_columns: List[NodeColumn]) -> List[Column]:
    """
    Finds all the specified node columns or raises if they don't exist
    """
    measure_columns = []
    for node_column in node_columns:
        node = get_node_by_name(session, node_column.node)
        available = [
            col for col in node.current.columns if col.name == node_column.column
        ]
        if len(available) == 0:
            raise DJDoesNotExistException(
                message=f"Column `{node_column.column}` does not exist on "
                f"node `{node_column.node}`",
            )
        measure_columns.extend(available)
    return measure_columns


@router.get("/measures/", response_model=List[str])
def list_measures(
    prefix: Optional[str] = None,
    session: Session = Depends(get_session),
) -> List[str]:
    """
    List all measures.
    """
    statement = select(Measure.name)
    if prefix:
        statement = statement.where(
            Measure.name.like(f"{prefix}%"),  # type: ignore  # pylint: disable=no-member
        )
    return session.execute(statement).scalars().all()


@router.get("/measures/{measure_name}", response_model=MeasureOutput)
def get_measure(
    measure_name: str, *, session: Session = Depends(get_session)
) -> MeasureOutput:
    """
    Get info on a measure.
    """
    measure = get_measure_by_name(session, measure_name, raise_if_not_exists=True)
    return measure


@router.post(
    "/measures/",
    response_model=MeasureOutput,
    status_code=201,
    name="Add a Measure",
)
def add_measure(
    data: CreateMeasure, *, session: Session = Depends(get_session)
) -> MeasureOutput:
    """
    Add a measure
    """
    measure = get_measure_by_name(session, data.name, raise_if_not_exists=False)
    if measure:
        raise DJAlreadyExistsException(message=f"Measure `{data.name}` already exists!")
    measure_columns = get_node_columns(session, data.columns)
    measure = Measure(
        name=data.name,
        display_name=data.display_name,
        description=data.description,
        columns=measure_columns,
        additive=data.additive,
    )
    session.add(measure)
    session.commit()
    session.refresh(measure)
    return measure


@router.patch(
    "/measures/{measure_name}",
    response_model=MeasureOutput,
    status_code=201,
    name="Edit a Measure",
)
def edit_measure(
    measure_name: str,
    data: EditMeasure,
    *,
    session: Session = Depends(get_session),
) -> MeasureOutput:
    """
    Edit a measure
    """
    measure = get_measure_by_name(session, measure_name, raise_if_not_exists=True)

    if data.description:
        measure.description = data.description

    if data.columns is not None:
        measure_columns = get_node_columns(session, data.columns)
        measure.columns = measure_columns

    if data.additive:
        measure.additive = data.additive

    if data.display_name:
        measure.display_name = data.display_name

    session.add(measure)
    session.commit()
    session.refresh(measure)
    return measure
