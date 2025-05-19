"""
Attributes related APIs.
"""

import logging
from typing import List

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.attributetype import AttributeType
from datajunction_server.errors import DJAlreadyExistsException, DJInvalidInputException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.attribute import (
    RESERVED_ATTRIBUTE_NAMESPACE,
    AttributeTypeBase,
    ColumnAttributes,
    MutableAttributeTypeFields,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["attributes"])


@router.get("/attributes/", response_model=List[AttributeTypeBase])
async def list_attributes(
    *,
    session: AsyncSession = Depends(get_session),
) -> List[AttributeTypeBase]:
    """
    List all available attribute types.
    """
    attributes = await AttributeType.get_all(session)
    return [AttributeTypeBase.from_orm(attr) for attr in attributes]


@router.post(
    "/attributes/",
    response_model=AttributeTypeBase,
    status_code=201,
    name="Add an Attribute Type",
)
async def add_attribute_type(
    data: MutableAttributeTypeFields,
    *,
    session: AsyncSession = Depends(get_session),
) -> AttributeTypeBase:
    """
    Add a new attribute type
    """
    if data.namespace == RESERVED_ATTRIBUTE_NAMESPACE:
        raise DJInvalidInputException(
            message="Cannot use `system` as the attribute type namespace as it is reserved.",
        )
    attribute_type = await AttributeType.get_by_name(session, data.name)
    if attribute_type:
        raise DJAlreadyExistsException(
            message=f"Attribute type `{data.name}` already exists!",
        )
    attribute_type = await AttributeType.create(session, data)
    return AttributeTypeBase.from_orm(attribute_type)


async def default_attribute_types(session: AsyncSession = Depends(get_session)):
    """
    Loads all the column attribute types that are supported by the system
    by default into the database.
    """
    defaults = [
        AttributeType(
            namespace=RESERVED_ATTRIBUTE_NAMESPACE,
            name=ColumnAttributes.PRIMARY_KEY.value,
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
            name=ColumnAttributes.DIMENSION.value,
            description="Points to a dimension attribute column",
            uniqueness_scope=[],
            allowed_node_types=[NodeType.SOURCE, NodeType.TRANSFORM],
        ),
        AttributeType(
            namespace=RESERVED_ATTRIBUTE_NAMESPACE,
            name=ColumnAttributes.HIDDEN.value,
            description=(
                "Points to a dimension column that's not useful "
                "for end users and should be hidden"
            ),
            uniqueness_scope=[],
            allowed_node_types=[NodeType.DIMENSION],
        ),
    ]
    default_attribute_type_names = {type_.name: type_ for type_ in defaults}

    # Update existing default attribute types
    statement = select(AttributeType).filter(
        AttributeType.name.in_(  # type: ignore
            set(default_attribute_type_names.keys()),
        ),
    )
    attribute_types = (await session.execute(statement)).scalars().all()
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
    session.add_all(
        [default_attribute_type_names[name] for name in new_types],
    )
    await session.commit()
