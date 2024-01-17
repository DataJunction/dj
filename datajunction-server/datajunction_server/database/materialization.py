"""Materialization database schema."""
from typing import TYPE_CHECKING, List, Optional, Union

import sqlalchemy as sa
from sqlalchemy import JSON, DateTime, Enum, ForeignKey, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.backfill import Backfill
from datajunction_server.database.base import Base
from datajunction_server.models.materialization import (
    DruidCubeConfig,
    GenericMaterializationConfig,
    MaterializationStrategy,
)
from datajunction_server.typing import UTCDatetime

if TYPE_CHECKING:
    from datajunction_server.database.node import NodeRevision


class Materialization(Base):  # pylint: disable=too-few-public-methods
    """
    Materialization configured for a node.
    """

    __tablename__ = "materialization"
    __table_args__ = (
        UniqueConstraint(
            "name",
            "node_revision_id",
            name="name_node_revision_uniq",
        ),
    )

    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )

    node_revision_id: Mapped[int] = mapped_column(
        ForeignKey(
            "noderevision.id",
            name="fk_materialization_node_revision_id_noderevision",
        ),
    )
    node_revision: Mapped["NodeRevision"] = relationship(
        "NodeRevision",
        back_populates="materializations",
    )

    name: Mapped[str]

    strategy: Mapped[Optional[MaterializationStrategy]] = mapped_column(
        Enum(MaterializationStrategy),
    )

    # A cron schedule to materialize this node by
    schedule: Mapped[str]

    # Arbitrary config relevant to the materialization job
    config: Mapped[
        Union[GenericMaterializationConfig, DruidCubeConfig]
    ] = mapped_column(
        JSON,
        default={},
    )

    # The name of the plugin that handles materialization, if any
    job: Mapped[str] = mapped_column(
        String,
        default="MaterializationJob",
    )

    deactivated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )

    backfills: Mapped[List[Backfill]] = relationship(
        back_populates="materialization",
        primaryjoin="Materialization.id==Backfill.materialization_id",
        cascade="all, delete",
    )
