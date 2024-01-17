"""Namespace database schema."""
from sqlalchemy import DateTime
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.base import Base
from datajunction_server.typing import UTCDatetime


class NodeNamespace(Base):  # pylint: disable=too-few-public-methods
    """
    A node namespace
    """

    __tablename__ = "nodenamespace"

    namespace: Mapped[str] = mapped_column(
        nullable=False,
        unique=True,
        primary_key=True,
    )
    deactivated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )
