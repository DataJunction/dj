"""Profile fingerprinting many downstream metrics without a database.

Run from the DJ repository with the server package installed:
    python datajunction-server/scripts/benchmark_fingerprints.py 6000 --profile

The mocked node lookup keeps this focused on Python work. It does not model
production database loading or the complexity of real metric definitions.
"""

import argparse
import asyncio
import cProfile
import logging
import pstats
import time
from unittest.mock import AsyncMock, MagicMock, patch

from datajunction_server.internal.deployment.fingerprints import (
    build_deployment_fingerprints,
)
from datajunction_server.models.deployment import MetricSpec, SourceSpec


async def run(count: int, profile: bool) -> None:
    """Build current and proposed fingerprints for ``count`` external metrics."""
    before = SourceSpec(
        namespace="ns",
        name="source",
        catalog="c",
        schema_="s",
        table="before",
    )
    after = before.model_copy(update={"table": "after"})
    metrics = [
        MetricSpec(
            namespace="other",
            name=f"metric_{index}",
            query="SELECT COUNT(*) FROM ns.source",
        )
        for index in range(count)
    ]
    nodes = []
    for metric in metrics:
        node = MagicMock()
        node.to_spec = AsyncMock(return_value=metric)
        nodes.append(node)

    profiler = cProfile.Profile() if profile else None
    started = time.perf_counter()
    with patch(
        "datajunction_server.internal.deployment.fingerprints.Node.get_by_names",
        AsyncMock(return_value=nodes),
    ):
        if profiler is not None:
            profiler.enable()
        current, proposed = await build_deployment_fingerprints(
            MagicMock(),
            {before.rendered_name: before},
            [after],
            [],
            additional_target_names=[metric.rendered_name for metric in metrics],
        )
        if profiler is not None:
            profiler.disable()

    print(
        f"count={count} elapsed={time.perf_counter() - started:.3f}s "
        f"current={len(current)} proposed={len(proposed)}",
    )
    if profiler is not None:
        pstats.Stats(profiler).strip_dirs().sort_stats("cumulative").print_stats(35)


def main() -> None:
    """Parse CLI arguments and run the benchmark."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("count", type=int, help="number of downstream metrics")
    parser.add_argument("--profile", action="store_true", help="show cProfile output")
    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING, format="%(message)s")
    logging.getLogger("datajunction_server.internal.deployment.fingerprints").setLevel(
        logging.INFO,
    )
    asyncio.run(run(args.count, args.profile))


if __name__ == "__main__":
    main()
