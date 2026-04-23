"""
One-off backfill for NodeRevision.derived_expression.

Most GraphQL queries that request metric's extractedMeasures can take a fast
path that reads NodeRevision.derived_expression directly instead of re-running
MetricComponentExtractor.extract(). New metrics get this populated via the
`derive_frozen_measures` background task triggered on node create/update
(internal/nodes.py:291), but metrics created before that was added have
derived_expression = NULL and force the slow extract() path.

Run this once after deploying the fast-path resolver to populate the column
for all existing metrics.

    python scripts/backfill_derived_expression.py
    # or: python scripts/backfill_derived_expression.py --batch-size 50 --dry-run

Idempotent: skips rows that already have derived_expression set. Safe to
re-run. Failures on individual metrics are logged and skipped so one bad
metric doesn't block the rest.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from sqlalchemy import select

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.internal.nodes import derive_frozen_measures
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import session_context

logger = logging.getLogger(__name__)


async def backfill(batch_size: int = 100, dry_run: bool = False) -> None:
    async with session_context() as session:
        stmt = (
            select(NodeRevision.id, Node.name)
            .join(Node, NodeRevision.node_id == Node.id)
            .where(
                Node.type == NodeType.METRIC,
                Node.current_version == NodeRevision.version,
                Node.deactivated_at.is_(None),
                NodeRevision.derived_expression.is_(None),
            )
        )
        targets = [(row.id, row.name) for row in (await session.execute(stmt)).all()]

    logger.info(
        "Found %d metric revisions missing derived_expression",
        len(targets),
    )
    if dry_run:
        for nr_id, name in targets[:20]:
            logger.info("  would backfill: nr_id=%d name=%s", nr_id, name)
        if len(targets) > 20:
            logger.info("  ... and %d more", len(targets) - 20)
        return

    total = len(targets)
    done = 0
    failed = 0
    for i in range(0, total, batch_size):
        batch = targets[i : i + batch_size]
        for nr_id, name in batch:
            try:
                # derive_frozen_measures opens its own session_context and
                # commits inside; safe to call in a loop.
                await derive_frozen_measures(nr_id)
                done += 1
            except Exception as exc:
                failed += 1
                logger.warning(
                    "backfill failed for nr_id=%d name=%s: %s",
                    nr_id,
                    name,
                    exc,
                )
        logger.info(
            "progress: %d/%d (done=%d failed=%d)",
            i + len(batch),
            total,
            done,
            failed,
        )

    logger.info("backfill complete: done=%d failed=%d", done, failed)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Progress log interval (default: 100)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count target rows without writing",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(backfill(batch_size=args.batch_size, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
