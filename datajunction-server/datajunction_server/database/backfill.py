# pylint: disable=unsubscriptable-object
"""Backfill database schema."""
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import JSON, BigInteger, ForeignKey, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.base import Base
from datajunction_server.models.partition import PartitionBackfill

if TYPE_CHECKING:
    from datajunction_server.database.materialization import Materialization


class Backfill(Base):  # type: ignore  # pylint: disable=too-few-public-methods
    """
    A backfill run is linked to a materialization config, where users provide the range
    (of a temporal partition) to backfill for the node.
    """

    __tablename__ = "backfill"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )

    # The column reference that this backfill is defined on
    materialization_id: Mapped[int] = mapped_column(
        ForeignKey(
            "materialization.id",
            name="fk_backfill_materialization_id_materialization",
        ),
    )
    materialization: Mapped["Materialization"] = relationship(
        back_populates="backfills",
        primaryjoin="Materialization.id==Backfill.materialization_id",
    )

    # Backfilled values and range
    spec: Mapped[List[PartitionBackfill]] = mapped_column(
        JSON,
        default=[],
    )

    urls: Mapped[Optional[List[str]]] = mapped_column(
        JSON,
        default=[],
    )
