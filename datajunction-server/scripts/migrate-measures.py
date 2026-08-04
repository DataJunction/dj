import asyncio

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import joinedload, selectinload, sessionmaker

from datajunction_server.database.node import Node, NodeRevision, NodeType
from datajunction_server.internal.nodes import derive_frozen_measures_bulk
from datajunction_server.utils import get_settings

settings = get_settings()


async def backfill_measures():
    engine = create_async_engine(settings.writer_db.uri)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with async_session() as session:
        async with session.begin():
            # Get all latest metric node revisions
            metric_revisions = [
                metric
                for metric in (
                    await session.execute(
                        sa.select(NodeRevision)
                        .join(
                            Node,
                            (NodeRevision.node_id == Node.id)
                            & (NodeRevision.version == Node.current_version),
                        )
                        .where(
                            NodeRevision.type == NodeType.METRIC,
                            sa.not_(NodeRevision.name.like("system.temp%")),
                        )
                        .options(
                            selectinload(NodeRevision.parents).options(
                                joinedload(Node.current),
                            ),
                            selectinload(NodeRevision.frozen_measures),
                        ),
                    )
                )
                .unique()
                .scalars()
                .all()
                if not metric.name.startswith("system.temp")
            ]
            print(f"Found {len(metric_revisions)} metric revisions")
            # Operator tool with no request user, so it uses the system-facing
            # bulk API (which also links the measures to each revision); the
            # per-revision entry point authorizes a user for WRITE.
            await derive_frozen_measures_bulk(
                session,
                [revision.id for revision in metric_revisions],
            )
            await session.commit()
            print(f"Derived frozen measures for {len(metric_revisions)} revisions")


if __name__ == "__main__":
    asyncio.run(backfill_measures())
