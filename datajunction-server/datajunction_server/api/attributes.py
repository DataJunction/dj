"""
Attributes related APIs.
"""

import logging
from typing import List

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from datajunction_server.database.attributetype import AttributeType
from datajunction_server.errors import DJAlreadyExistsException, DJException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.attribute import (
    RESERVED_ATTRIBUTE_NAMESPACE,
    AttributeTypeBase,
    MutableAttributeTypeFields,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["attributes"])


@router.get("/attributes/", response_model=List[AttributeTypeBase])
def list_attributes(
    *, session: Session = Depends(get_session)
) -> List[AttributeTypeBase]:
    """
    List all available attribute types.
    """
    attributes = session.execute(select(AttributeType)).scalars().all()
    return [AttributeTypeBase.from_orm(attr) for attr in attributes]


@router.post(
    "/attributes/",
    response_model=AttributeTypeBase,
    status_code=201,
    name="Add an Attribute Type",
)
def add_attribute_type(
    data: MutableAttributeTypeFields, *, session: Session = Depends(get_session)
) -> AttributeTypeBase:
    """
    Add a new attribute type
    """
    if data.namespace == RESERVED_ATTRIBUTE_NAMESPACE:
        raise DJException(
            message="Cannot use `system` as the attribute type namespace as it is reserved.",
        )
    statement = select(AttributeType).where(AttributeType.name == data.name)
    attribute_type = session.execute(statement).unique().one_or_none()
    if attribute_type:
        raise DJAlreadyExistsException(
            message=f"Attribute type `{data.name}` already exists!",
        )
    attribute_type = AttributeType(
        namespace=data.namespace,
        name=data.name,
        description=data.description,
        allowed_node_types=data.allowed_node_types,
        uniqueness_scope=data.uniqueness_scope if data.uniqueness_scope else [],
    )
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
    attribute_types = session.execute(statement).scalars().all()
    for type_ in attribute_types:
        # if type_:  # pragma: no cover
        type_.name = default_attribute_type_names[type_.name].name
        type_.namespace = default_attribute_type_names[type_.name].namespace
        type_.description = default_attribute_type_names[type_.name].description
        type_.allowed_node_types = default_attribute_type_names[
            type_.name
        ].allowed_node_types
        type_.uniqueness_scope = default_attribute_type_names[
            type_.name
        ].uniqueness_scope
        session.add(type_)

    # Add new default attribute types
    new_types = set(default_attribute_type_names.keys()) - {
        type_.name for type_ in attribute_types if type_
    }
    session.bulk_save_objects(
        [default_attribute_type_names[name] for name in new_types],
    )
    session.commit()
