"""Attribute type database schema."""
from typing import TYPE_CHECKING, List, Optional

import sqlalchemy as sa
from sqlalchemy import UniqueConstraint, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.base import Base
from datajunction_server.models.attribute import MutableAttributeTypeFields

if TYPE_CHECKING:
    from datajunction_server.database.column import Column


class AttributeType(
    Base,
):  # pylint: disable=too-few-public-methods,unsubscriptable-object
    """
    Available attribute types for column metadata.
    """

    __tablename__ = "attributetype"
    __table_args__ = (UniqueConstraint("namespace", "name"),)

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )
    namespace: Mapped[str] = mapped_column(nullable=False, default="system")
    name: Mapped[str] = mapped_column(nullable=False)
    description: Mapped[str] = mapped_column(nullable=False)
    allowed_node_types: Mapped[List[str]] = mapped_column(sa.JSON, nullable=True)
    uniqueness_scope: Mapped[List[str]] = mapped_column(sa.JSON, nullable=True)

    def __hash__(self):
        return hash(self.id)

    @classmethod
    async def get_all(cls, session: AsyncSession):
        """
        Get all AttributeTypes.
        """
        stmt = select(cls)
        result = await session.execute(stmt)
        return result.scalars().all()

    @classmethod
    async def get_by_name(
        cls,
        session: AsyncSession,
        name: str,
    ) -> Optional["AttributeType"]:
        """
        Get an AttributeType by name.
        """
        stmt = select(cls).where(cls.name == name)
        result = await session.execute(stmt)
        return result.unique().one_or_none()

    @classmethod
    async def create(
        cls,
        session: AsyncSession,
        new_obj: MutableAttributeTypeFields,
    ) -> Optional["AttributeType"]:
        """
        Get an AttributeType by name.
        """
        attribute_type = AttributeType(
            namespace=new_obj.namespace,
            name=new_obj.name,
            description=new_obj.description,
            allowed_node_types=new_obj.allowed_node_types,
            uniqueness_scope=new_obj.uniqueness_scope
            if new_obj.uniqueness_scope
            else [],
        )
        session.add(attribute_type)
        await session.commit()
        await session.refresh(attribute_type)
        return attribute_type


class ColumnAttribute(
    Base,
):  # pylint: disable=too-few-public-methods,unsubscriptable-object
    """
    Column attributes.
    """

    __tablename__ = "columnattribute"
    __table_args__ = (UniqueConstraint("attribute_type_id", "column_id"),)

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
    )

    attribute_type_id: Mapped[int] = mapped_column(
        sa.ForeignKey(
            "attributetype.id",
            name="fk_columnattribute_attribute_type_id_attributetype",
        ),
    )
    attribute_type: Mapped[AttributeType] = relationship(
        foreign_keys=[attribute_type_id],
        lazy="joined",
    )

    column_id: Mapped[Optional[int]] = mapped_column(
        sa.ForeignKey("column.id", name="fk_columnattribute_column_id_column"),
    )
    column: Mapped[Optional["Column"]] = relationship(
        back_populates="attributes",
        foreign_keys=[column_id],
    )
