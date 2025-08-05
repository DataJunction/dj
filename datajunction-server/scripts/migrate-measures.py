import asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, selectinload, joinedload

from datajunction_server.database.node import NodeRevision, NodeType, Node
from datajunction_server.internal.nodes import derive_frozen_measures
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
            for idx, revision in enumerate(metric_revisions):
                try:
                    print(
                        f"[{idx + 1}/{len(metric_revisions)}] Processing metric revision {revision.name}@{revision.version}",
                    )
                    derived_measures = [
                        m for m in await derive_frozen_measures(session, revision) if m
                    ]
                    print(
                        f"[{idx + 1}/{len(metric_revisions)}] Derived the following frozen measures: {[m.name for m in derived_measures]}",
                    )

                    for frozen_measure in derived_measures:
                        session.add(frozen_measure)
                        with session.no_autoflush:
                            if frozen_measure not in revision.frozen_measures:
                                revision.frozen_measures.append(frozen_measure)
                        print(
                            f"[{idx + 1}/{len(metric_revisions)}] Added frozen measures: {[m.name for m in derived_measures]}",
                        )
                    session.add(revision)
                    print("---")
                except Exception as exc:
                    print(
                        "[{idx+1}/{len(metric_revisions)}] Failed to process",
                        derived_measures,
                    )
                    print(exc)
                    raise exc

            await session.commit()


if __name__ == "__main__":
    asyncio.run(backfill_measures())
