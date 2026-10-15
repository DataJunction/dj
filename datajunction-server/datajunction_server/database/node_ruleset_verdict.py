"""Per-node ruleset verdicts, recorded by the deploy that evaluated them."""

import datetime
from functools import partial

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
    delete,
)
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.database.base import Base
from datajunction_server.typing import UTCDatetime


class NodeRulesetVerdict(Base):
    """
    What one ruleset concluded for one node, as of the deploy that last
    evaluated it.

    A projection of the verdicts already stored on the deployment, kept here so
    a node list can filter on the tiers a node has reached. The deployment row
    holds the authoritative record; this table is rebuilt from it.

    `node_version` is the version the verdict was computed from, so a row that
    no longer matches the node's `current_version` is recognisably stale rather
    than silently wrong.
    """

    __tablename__ = "noderulesetverdict"
    __table_args__ = (
        UniqueConstraint("node_id", "ruleset"),
        Index("ix_noderulesetverdict_node_id", "node_id"),
        # The list filter asks for one ruleset's verdict across many nodes, so
        # the pair is what gets scanned rather than either column alone.
        Index("ix_noderulesetverdict_ruleset_verdict", "ruleset", "verdict"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    node_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("node.id", ondelete="CASCADE"),
        nullable=False,
    )
    ruleset: Mapped[str] = mapped_column(String, nullable=False)
    verdict: Mapped[str] = mapped_column(String, nullable=False)
    node_version: Mapped[str] = mapped_column(String, nullable=False)
    updated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.datetime.now, datetime.UTC),
        onupdate=partial(datetime.datetime.now, datetime.UTC),
    )

    @classmethod
    async def replace_for_nodes(
        cls,
        session: AsyncSession,
        verdicts: dict[int, tuple[str, list[tuple[str, str]]]],
    ) -> None:
        """
        Replace the stored verdicts for each node in `verdicts`, which maps a
        node id to its version and its `(ruleset, verdict)` pairs.

        Nodes are replaced wholesale rather than upserted row by row, so a
        ruleset dropped from the manifest leaves no row behind.
        """
        if not verdicts:
            return

        await session.execute(
            delete(cls).where(cls.node_id.in_(list(verdicts))),
        )
        session.add_all(
            [
                cls(
                    node_id=node_id,
                    ruleset=ruleset,
                    verdict=verdict,
                    node_version=version,
                )
                for node_id, (version, pairs) in verdicts.items()
                for ruleset, verdict in pairs
            ],
        )
