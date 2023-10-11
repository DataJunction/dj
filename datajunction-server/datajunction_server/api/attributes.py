"""
Attributes related APIs.
"""

import logging
from typing import List

from fastapi import Depends
from sqlmodel import Session, select

from datajunction_server.errors import DJAlreadyExistsException, DJException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.attribute import (
    RESERVED_ATTRIBUTE_NAMESPACE,
    AttributeType,
    MutableAttributeTypeFields,
)
from datajunction_server.models.node import NodeType
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["attributes"])


@router.get("/attributes/", response_model=List[AttributeType])
def list_attributes(*, session: Session = Depends(get_session)) -> List[AttributeType]:
    """
    List all available attribute types.
    """
    return session.exec(select(AttributeType)).all()


@router.post(
    "/attributes/",
    response_model=AttributeType,
    status_code=201,
    name="Add an Attribute Type",
)
def add_attribute_type(
    data: MutableAttributeTypeFields, *, session: Session = Depends(get_session)
) -> AttributeType:
    """
    Add a new attribute type
    """
    if data.namespace == RESERVED_ATTRIBUTE_NAMESPACE:
        raise DJException(
            message="Cannot use `system` as the attribute type namespace as it is reserved.",
        )
    statement = select(AttributeType).where(AttributeType.name == data.name)
    attribute_type = session.exec(statement).unique().one_or_none()
    if attribute_type:
        raise DJAlreadyExistsException(
            message=f"Attribute type `{data.name}` already exists!",
        )
    attribute_type = AttributeType.from_orm(data)
    session.add(attribute_type)
    session.commit()
    session.refresh(attribute_type)
    return attribute_type


def default_attribute_types(session: Session = Depends(get_session)):
    """
    Loads all the column attribute types that are supported by the system
    by default into the database.
    """
    defaults = [
        AttributeType(
            namespace=RESERVED_ATTRIBUTE_NAMESPACE,
            name="primary_key",
            description="Points to a column which is part of the primary key of the node",
            uniqueness_scope=[],
            allowed_node_types=[
                NodeType.SOURCE,
                NodeType.TRANSFORM,
                NodeType.DIMENSION,
            ],
        ),
        AttributeType(
            namespace=RESERVED_ATTRIBUTE_NAMESPACE,
            name="event_time",
            description="Points to a column which represents the time of the event in a given "
            "fact related node. Used to facilitate proper joins with dimension node "
            "to match the desired effect.",
            uniqueness_scope=["node", "column_type"],
            allowed_node_types=[NodeType.SOURCE, NodeType.TRANSFORM],
        ),
        AttributeType(
            namespace=RESERVED_ATTRIBUTE_NAMESPACE,
            name="effective_time",
            description="Points to a column which represents the effective time of a row in a "
            "dimension node. Used to facilitate proper joins with fact nodes"
            " on event time.",
            uniqueness_scope=["node", "column_type"],
            allowed_node_types=[NodeType.DIMENSION],
        ),
        AttributeType(
            namespace=RESERVED_ATTRIBUTE_NAMESPACE,
            name="expired_time",
            description="Points to a column which represents the expired time of a row in a "
            "dimension node. Used to facilitate proper joins with fact nodes "
            "on event time.",
            uniqueness_scope=["node", "column_type"],
            allowed_node_types=[NodeType.DIMENSION],
        ),
        AttributeType(
            namespace=RESERVED_ATTRIBUTE_NAMESPACE,
            name="dimension",
            description="Points to a dimension attribute column",
            uniqueness_scope=[],
            allowed_node_types=[NodeType.SOURCE, NodeType.TRANSFORM],
        ),
    ]
    default_attribute_type_names = {type_.name: type_ for type_ in defaults}

    # Update existing default attribute types
    statement = select(AttributeType).filter(
        # pylint: disable=no-member
        AttributeType.name.in_(  # type: ignore
            set(default_attribute_type_names.keys()),
        ),
    )
    attribute_types = session.exec(statement).all()
    for type_ in attribute_types:
        updated_type = default_attribute_type_names[type_.name].dict(
            exclude_unset=True,
        )
        type_ = type_.update(updated_type)
        session.add(type_)

    # Add new default attribute types
    new_types = set(default_attribute_type_names.keys()) - {
        type_.name for type_ in attribute_types
    }
    for name in new_types:
        session.add(default_attribute_type_names[name])
    session.commit()
