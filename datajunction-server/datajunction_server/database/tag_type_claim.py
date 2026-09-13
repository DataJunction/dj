"""Registry of tag types claimed by a namespace."""

import datetime
from functools import partial

from sqlalchemy import BigInteger, DateTime, ForeignKey, String, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.base import Base
from datajunction_server.typing import UTCDatetime


class TagTypeClaim(Base):
    """
    A tag type claimed by a namespace: only that namespace's deployments may
    create tags of the type. A type can be claimed by at most one namespace.
    """

    __tablename__ = "tagtypeclaim"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    tag_type: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    namespace: Mapped[str] = mapped_column(String, nullable=False)
    created_by_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("users.id"),
        default=None,
    )
    updated_by_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("users.id"),
        default=None,
    )
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.datetime.now, datetime.UTC),
    )
    updated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.datetime.now, datetime.UTC),
        onupdate=partial(datetime.datetime.now, datetime.UTC),
    )

    @classmethod
    async def get_owners(
        cls,
        session: AsyncSession,
        tag_types: list[str] | None = None,
    ) -> dict[str, str]:
        """
        Return ``{tag_type: owning namespace}`` for the requested types, or for
        every claim when no types are given.
        """
        statement = select(cls.tag_type, cls.namespace)
        if tag_types is not None:
            if not tag_types:
                return {}
            statement = statement.where(cls.tag_type.in_(tag_types))
        rows = (await session.execute(statement)).all()
        return {tag_type: namespace for tag_type, namespace in rows}
