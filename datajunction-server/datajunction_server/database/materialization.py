"""Materialization database schema."""

from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import (
    JSON,
    DateTime,
    Enum,
    ForeignKey,
    String,
    UniqueConstraint,
    and_,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import (
    Mapped,
    joinedload,
    mapped_column,
    relationship,
    selectinload,
)

from datajunction_server.database.backfill import Backfill
from datajunction_server.database.base import Base
from datajunction_server.database.column import Column
from datajunction_server.models.cube_materialization import (
    DRUID_CUBE_V3_MATERIALIZATION_NAME,
    DruidCubeV3Config,
)
from datajunction_server.models.materialization import (
    DruidMeasuresCubeConfig,
    GenericMaterializationConfig,
    MaterializationStrategy,
)
from datajunction_server.typing import UTCDatetime

if TYPE_CHECKING:
    from datajunction_server.database.node import NodeRevision


class Materialization(Base):
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
        sa.Index("ix_materialization_node_revision_id", "node_revision_id"),
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
            ondelete="CASCADE",
        ),
    )
    node_revision: Mapped["NodeRevision"] = relationship(
        "NodeRevision",
        back_populates="materializations",
        passive_deletes=True,
    )

    name: Mapped[str]

    strategy: Mapped[MaterializationStrategy | None] = mapped_column(
        Enum(MaterializationStrategy),
    )

    # A cron schedule to materialize this node by
    schedule: Mapped[str]

    # Arbitrary config relevant to the materialization job
    config: Mapped[
        GenericMaterializationConfig | DruidMeasuresCubeConfig | DruidCubeV3Config
    ] = mapped_column(
        JSON,
        default={},
    )

    # The name of the plugin that handles materialization, if any
    job: Mapped[str] = mapped_column(
        String,
        default="MaterializationJob",
    )

    # Workflows the query service reported when it scheduled this
    workflow_names: Mapped[list[str] | None] = mapped_column(
        JSON,
        nullable=True,
        default=None,
    )

    deactivated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )

    backfills: Mapped[list[Backfill]] = relationship(
        back_populates="materialization",
        primaryjoin="Materialization.id==Backfill.materialization_id",
        cascade="all, delete",
        lazy="selectin",
    )

    @property
    def backfill_workflow(self) -> str | None:
        """
        The recorded workflow a backfill run belongs to, if the query service made
        one.

        Picked by its `.backfill` suffix rather than by position: the scheduled
        workflow is always reported, the backfill one only for an incremental
        strategy, so a one-element list is the ordinary shape and indexing into it
        would hand the scheduler its own scheduled workflow to backfill on.
        """
        return next(
            (name for name in self.workflow_names or [] if name.endswith(".backfill")),
            None,
        )

    @property
    def is_cube_planner(self) -> bool:
        """
        Whether this row was written by the cube planner (v3) rather than by the
        fused cube materialization path.

        The two dialects are indistinguishable by job: `POST /cubes/{name}/materialize`
        persists the same `DruidCubeMaterializationJob` the fused path derives. What
        separates them is the row name the planner writes and the version
        discriminator its config carries, either of which is enough to recognize.

        The name is the authoritative signal, since the planner is the only writer of
        it. The `version` check is a fallback for rows written before that name
        settled, so a row matching either one is treated as the planner's.
        """
        config = self.config if isinstance(self.config, dict) else {}
        return (
            self.name == DRUID_CUBE_V3_MATERIALIZATION_NAME
            or config.get("version") == "v3"
        )

    @classmethod
    async def get_by_names(
        cls,
        session: AsyncSession,
        node_revision_id: int,
        materialization_names: list[str],
    ) -> list["Materialization"]:
        """
        Get materializations by name and node revision id.
        """
        from datajunction_server.database.node import Node, NodeRevision
        from datajunction_server.database.user import User

        statement = (
            select(cls)
            .where(
                and_(
                    cls.name.in_(materialization_names),
                    cls.node_revision_id == node_revision_id,
                ),
            )
            .options(
                joinedload(cls.node_revision).options(
                    joinedload(NodeRevision.columns).joinedload(Column.partition),
                    selectinload(NodeRevision.node)
                    .selectinload(Node.owners)
                    .load_only(User.username, User.kind)
                    .raiseload("*"),
                ),
            )
        )
        result = await session.execute(statement)
        return result.unique().scalars().all()
